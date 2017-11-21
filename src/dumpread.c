/* Michael Coates : mcoates@wayfair.com : @outerwear
    Reading and converting a binary dump file from Redis
    HOW TO RUN:
        dumpread [filename1] [filename2] [optional:full] [optional:silent]
    ARGUMENTS:
        [filename1] - RDB file to be parsed
        [filename2] - Output file to contain all key information
        [full]      - Optional. Includes value in out file.
        [silent]    - Optional. Prevents anything being written to STDOUT.
    RETURN CODES:
        0 - Success!
        1 - Not enough arguments passed in
        2 - Bad file descriptor. Could be wrong path or permissions issue.
        3 - Not RDB file type.
    NOTES: 
        Ziplists use 0xFF to indicate end so if that is not grabbed correctly we may prematurely exit.
        But where are my keys?! Redis bgsave will not save expired keys. However the redis-cli info
            will count the expired ones that haven't been freed. So there will be a discrepancy 
            between redis-cli info keyspace and total key count from this
        Compiling with DEBUG will result in very verbose messages. It is recommended to do this 
            only for rdb files of smaller size (a few GB).


    CHANGES TO RDB WITH VER 4
        - 64bitlen support
        - few more aux stuff
        - sorted sets use raw double format (64bit)
        - redis modules... (wtf are these)
 */

#define _GNU_SOURCE     /* for asprintf() */
#include "lzf.h"
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>


#define BUFFERSIZE          10
#define PTRSZ               sizeof(void*)
#define ULSZ                sizeof(unsigned long)
#define INTSZ               sizeof(int)
#define MASK                0x3F
#define SPACE_FOR_NULL      1
#define maxint              11

/* Overhead for redis data types 
 * Estimations gathered from https://github.com/sripathikrishnan/redis-rdb-tools/ and Redis source code
 * I assume 64 bit, or at the very least you are running this on a similar architecture as the Redis instance
 * Redis Object: pointer + int64 
 * String: 
 * List: long + 5 pointers
 * List Node: 3 pointers
 * Hash: 2*(3 unsigned longs + 1 pointer) + int + long + 2 pointers * (worst case of table rehash calculated as 1.5)
 * Sorted Set: 
 * Quicklist:
 * Quickitem: number of ziplist entries * this
 * Dict Entry: 
 * Expiration: int64, 2 pointers, int64 
 */
#define ROBJ_OH             PTRSZ + 8 
#define STR_OH              PTRSZ * 2 
#define LIST_OH             ULSZ+(5*PTRSZ) /* Also OH for set */
#define LN_OH               (3*PTRSZ)
#define HASH_OH             4+(7*8)+(4*8)+(8*1.5)           
#define SSET_OH             56                              
#define QL_OH               (3*PTRSZ)+(2*4)
#define QI_OH               (4*8)+8+(2*4)
#define DICT_OH             (8)+(8*2)                       
#define EXP_OH              8+(2*PTRSZ)+8                       

#ifdef DEBUG
    #define DEBUG           1
#else
    #define DEBUG           0
#endif
#define debug_print(...)    do{if(DEBUG)fprintf(stderr,__VA_ARGS__);}while(0)
#define print_usage         fprintf(stderr,"Usage : dumpread [rdb file] [out file] [optional:full] [optional:silent]\n")

/*
    aux tells us if it is an aux key which has special str_enc()
*/
struct {
    unsigned int x : 1;
} aux;

/* Arg vars */
struct {
    uint8_t noisy : 1;
    uint8_t full  : 1;
} args;

/*
    KI : Key Info Structure
        str = name/value
        size = size in bytes
    create_KI()
        Allocate space for a new KI with initialized values
*/
struct KI {
    unsigned long long size;
    char *str;
};

static struct KI* create_KI(){
    struct KI *key = malloc(sizeof(struct KI));
    if(key == NULL)
        return NULL;
    key->str = NULL;
    key->size = 0;
    return key;
}

uint64_t strtou64(char *s){
    uint64_t x;
    for(x=0; (unsigned)*s-'0'<10; s++)
        x=(x*10)+(*s-'0');
    return x;
}

static char* get_zl_entry(char *c, unsigned long *offset){
    /* 
        Get Ziplist Entry
        Passed in the buffer, c, containing a string of bytes
        First byte is the length of the previous item
        Advance pointer, check value to match one of 9 conditions
        00------ : String, size = remaining 6 bits
        01------ : String, size = remaining 6 bits combined with next byte to make 14 bits
        10------ : String, size = next 4 bytes in big endian
        1100---- : Int, next 2 bytes make a signed 16 bit int 
        1101---- : Int, next 4 bytes make a signed 32 bit int
        1110---- : Int, next 8 bytes make a signed 64 bit int
        11110000 : Int, next 3 bytes make a signed 24 bit int
        11111110 : Int, next 1 byte makes a signed  8 bit int
        11110001 -> 11111101 : Current byte used to extract a 4 bit int (subtract 1 to get true value)
    */
    char *entry = NULL;
    if((unsigned char)c[0] == 254){
        c+=4;
        *offset+=4; 
    }
    c++;
    *offset += 1;
    if((unsigned char)c[0] >= 0 && (unsigned char)c[0] < 64){
        uint8_t slen = 0;
        slen |= (c[0] & MASK);
        debug_print("DEBUG: get_zl_entry\n\t1: %.2X -> %" PRIu8 "\n",c[0],slen);
        if(slen > 0){
            entry = malloc(sizeof(char)*slen+SPACE_FOR_NULL);
            if(entry == NULL){
                debug_print("ERROR: Could not malloc space for %" PRIu8 " bytes\n",slen);
                return NULL;
            }
            memset(entry,'\0',slen+SPACE_FOR_NULL);
            memcpy(entry,c+1,slen);
        }
        *offset += (slen+1);
    } else if((unsigned char)c[0] > 63 && (unsigned char)c[0] < 128){
        uint16_t slen = 0;
        /* 
            This part was a strange bug to track down so here is an explanation:
            After decompressing, the bytes represented in the char array are potentially 
            represented as 0xFFFFFF80 instead of just the last two bytes (80 in this example).
            This caused a problem with the above because it would do slen |= 0xFFFFFF00 and 
            not slen |= 0x80 as expected. Then the length would become something absurd
            and cause a seg fault. So I just mask the beginning to be zeros which would have 
            no effect on an already proper byte representation.
        */
        slen |= ((c[0] & MASK) << 8u); 
        slen |= (c[1] & 0x000000FF);
        debug_print("DEBUG: get_zl_entry\n\t2: %.2X %.2X -> %" PRIu16 "\n",c[0],c[1],slen);
        entry = malloc(sizeof(char)*slen+SPACE_FOR_NULL);
        if(entry == NULL){
            debug_print("ERROR: Could not malloc space for %" PRIu16 " bytes\n",slen);
            return NULL;
        }
        memcpy(entry,c+2,slen);
        entry[slen] = '\0';
        *offset += (2+slen);
    } else if((unsigned char)c[0] > 127 && (unsigned char)c[0] < 192){
        uint32_t val = 0, slen = 0;
        memcpy(&val,c+1,4);
        /* Endian swap */
        slen =  ((val & 0x000000ff) << 24u) |
                ((val & 0x0000ff00) <<  8u) |
                ((val & 0x00ff0000) >>  8u) |
                ((val & 0xff000000) >> 24u) ;
        debug_print("DEBUG: get_zl_entry\n\t3: %.2X -> %X %" PRIu32 "\n",c[0],val,slen);
        entry = malloc(sizeof(char)*slen+SPACE_FOR_NULL);
        if(entry == NULL){
            debug_print("ERROR: Could not malloc space for %" PRIu32 " bytes\n",slen);
            return NULL;
        }
        memcpy(entry,c+5,slen);
        entry[slen] = '\0';
        *offset += (5+slen);
    } else if((unsigned char)c[0] > 191 && (unsigned char)c[0] < 208){
        int16_t i = 0;
        memcpy(&i,c+1,2);
        debug_print("DEBUG: get_zl_entry\n\t4: %.2X -> %" PRId16 "\n",c[0],i);
        entry = malloc(sizeof(char)*5+SPACE_FOR_NULL);
        memset(entry,'\0',5+SPACE_FOR_NULL);
        sprintf(entry,"%" PRId16,i);
        *offset += 3;
    } else if((unsigned char)c[0] > 207 && (unsigned char)c[0] < 224){
        int32_t i = 0;
        memcpy(&i,c+1,4);
        debug_print("DEBUG: get_zl_entry\n\t5: %.2X -> %" PRId32 "\n",c[0],i);
        entry = malloc(sizeof(char)*11+SPACE_FOR_NULL);
        memset(entry,'\0',12);
        sprintf(entry,"%" PRId32,i);
        *offset += 5;
    } else if((unsigned char)c[0] > 223 && (unsigned char)c[0] < 240){
        int64_t i = 0;
        memcpy(&i,c+1,8);
        debug_print("DEBUG: get_zl_entry\n\t6: %.2X -> %" PRId64 "\n",c[0],i);
        entry = malloc(sizeof(char)*20+SPACE_FOR_NULL);
        memset(entry,'\0',21);
        sprintf(entry,"%" PRId64,i);
        *offset += 9;
    } else if((unsigned char)c[0] > 240 && (unsigned char)c[0] < 254){
        uint8_t i = (c[0] & 0x0F) - 1;
        debug_print("DEBUG: get_zl_entry\n\t7: %.2X -> %" PRIu8 "\n",c[0],i);
        entry = malloc(sizeof(char)*3+SPACE_FOR_NULL);
        memset(entry,'\0',4);
        sprintf(entry,"%" PRIu8,i);
        *offset += 1;
    } else if((unsigned char)c[0] == 240){
        int32_t int24 = 0;
        memcpy(&int24,c+1,3);
        debug_print("DEBUG: get_zl_entry\n\t8: %.2X -> %" PRId32 "\n",c[0],int24);
        entry = malloc(sizeof(char)*11+SPACE_FOR_NULL);
        memset(entry,'\0',12);
        sprintf(entry,"%" PRId32,int24);
        *offset += 4;
    } else if((unsigned char)c[0] == 254){
        int8_t int8 = 0;
        memcpy(&int8,c+1,1);
        debug_print("DEBUG: get_zl_entry\n\t9: %.2X -> %" PRId8 "\n",c[0],int8);
        entry = malloc(sizeof(char)*3+SPACE_FOR_NULL);
        memset(entry,'\0',4);
        sprintf(entry,"%" PRId8,int8);
        *offset += 2;
    }
    return entry;
}

static int load_compressed(unsigned char *c, unsigned int clen, FILE *fd){
    return fread(c,1,clen,fd);
}

static unsigned long long get_length(unsigned char *buffer, FILE *fd){
    unsigned int val = 0;
    unsigned long len = 0;
    /*  Everything uses Redis length encoding:
            00 : next six bits represent length
            01 : read additional byte from stream, combined 14 bits represent length
            10 : remaining 6 bits discarded, read 4 more bytes and they represent length
            11 : next object encoded in special format. Remaining 6 bits indicate format.
    */
    if (buffer[0] == 0x80) {
        debug_print("get_length() case 80\n");
        fread(&val,1,4,fd);
        len |= (val & 0x000000ff) << 24u;
        len |= (val & 0x0000ff00) << 8u;
        len |= (val & 0x00ff0000) >> 8u;
        len |= (val & 0xff000000) >> 24u;
        return len;
    } else if (buffer[0] == 0x81) {
        debug_print("get_length() case 81\n");
        unsigned long long dlen = 0;
        unsigned long long dval = 0;
        fread(&dval,1,8,fd);
        dlen |= (dval & 0x00000000000000ff) << 56u;
        dlen |= (dval & 0x000000000000ff00) << 40u;
        dlen |= (dval & 0x0000000000ff0000) << 24u;
        dlen |= (dval & 0x00000000ff000000) << 8u;
        dlen |= (dval & 0x000000ff00000000) >> 8u;
        dlen |= (dval & 0x0000ff0000000000) >> 24u;
        dlen |= (dval & 0x00ff000000000000) >> 40u;
        dlen |= (dval & 0xff00000000000000) >> 56u;
        return dlen;

    } else {
        switch(buffer[0] & 0xC0){
            case 0x00:
                debug_print("get_length() case 00\n");
                len |= (buffer[0] & MASK);
                return len;
            case 0x40:
                debug_print("get_length() case 40\n");
                fread(buffer+1,1,1,fd);
                /* 
                   buffer[0] = 00111111 
                   buffer[1] = 11111111
                   combine into 2 byte thing...
                 */
                debug_print("bytes : %x %x\n", buffer[0], buffer[1]);
                len = ((buffer[0] & MASK) << 8u) | buffer[1];
                return len;
            case 0x80:
                debug_print("get_length() case 80\n");
                fread(&val,1,4,fd);
                len |= (val & 0x000000ff) << 24u;
                len |= (val & 0x0000ff00) << 8u;
                len |= (val & 0x00ff0000) >> 8u;
                len |= (val & 0xff000000) >> 24u;
                return len;
            case 0xC0:
                debug_print("get_length() case C0\n");
                /* special format to be handled separately */
                return 0; 
        }
        debug_print("get_length() case bad\n");
        return 0;
    }
}

/*  Begin Encoding Functions  */

static struct KI* str_enc(FILE *fd){
    int8_t x = 0;
    int16_t y = 0;
    int32_t z = 0;
    struct KI *key = NULL;
    unsigned long long size;
    unsigned long long unlen;
    unsigned char buffer[BUFFERSIZE], *c;
    key = create_KI();
    if(key == NULL)
        return NULL;
    memset(buffer,0x00,BUFFERSIZE);
    fread(buffer,1,1,fd);
    if(ferror(fd)){
        fprintf(stderr,"ERROR : Failed to read byte to obtain length\n");
        free(key);
        return NULL;
    }
    key->size = get_length(buffer,fd);
    debug_print("DEBUG: str_enc\tsize : %llu\n",key->size);
    if(key->size == 0){
        /*  
            Special Format Area!
            Check next byte because it could be the following:
                Scenario 1: Special format needs to be handled here
                Scenario 2: Actually size 0 in which case wtf is a size 0 key
            Either way we read remaining 6 bits
        */
        switch(buffer[0] & MASK){
            case 0:
                /* 
                    8 bit integer follows (0,1,2 are actually just numbers in string format)
                */
                debug_print("DEBUG: str_enc\tcase 0\n");
                if(buffer[0] == 0x00){
                    if(args.full || aux.x){
                        key->str = malloc(sizeof(char));
                        key->str[0] = '\0';
                    }
                } else {
                    fread(&x,1,1,fd);
                    if(args.full || aux.x){ 
                        key->str = malloc(maxint);
                        memset(key->str,'\0',maxint);
                        sprintf(key->str,"%d",x);
                    }
                    key->size = 1;
                }
                if(args.full)
                    debug_print("DEBUG: str_enc\tvalue: %s\n",key->str);
                return key;
            case 1:
                /* 16 bit int */
                debug_print("DEBUG: str_enc\tcase 1\n");
                fread(&y,1,2,fd);
                if(args.full || aux.x){
                    key->str = malloc(maxint);
                    memset(key->str,'\0',maxint);
                    sprintf(key->str,"%d",y);
                    debug_print("DEBUG: str_enc\tvalue: %s\n",key->str);
                }
                key->size = 2;
                return key;
            case 2:
                /* 32 bit int */
                debug_print("DEBUG: str_enc\tcase 2\n");
                fread(&z,1,4,fd);
                if(args.full || aux.x){ 
                    key->str = malloc(maxint);
                    memset(key->str,'\0',maxint);
                    sprintf(key->str,"%d",z);
                    debug_print("DEBUG: str_enc\tvalue: %s\n",key->str);
                }
                key->size = 4;
                return key;
            case 3:
                /* 
                    Compressed String 
                    Compressed length, size, is read using get_length()
                    Uncompressed length, unlen, is read using get_length()
                        size bytes are read from stream
                        decompress using lzf
                    Let's link Redis lzf_decompress function because it is easier
                */
                debug_print("DEBUG: str_enc\tcase 3\n");
                memset(buffer,0x00,BUFFERSIZE);
                fread(buffer,1,1,fd);
                size = get_length(buffer,fd);
                debug_print("DEBUG: str_enc getlength() size %llu\n",size);
                c = malloc(sizeof(char)*size+SPACE_FOR_NULL);
                fread(buffer,1,1,fd);
                unlen = get_length(buffer,fd);
                debug_print("DEBUG: str_enc getlength() unlen %llu\n",unlen);
                key->str = malloc(sizeof(char)*unlen+SPACE_FOR_NULL);
                key->size = unlen;
                memset(key->str,'\0',unlen+1);
                if(args.full || aux.x){
                    if(load_compressed(c,size,fd) == 0){
                        /* Couldn't get compressed string */
                        fprintf(stderr,"ERROR : Could not get compressed string\n");
                        free(c);
                        free(key->str);
                        break;
                    }     
                    if(lzf_decompress(c,size,key->str,unlen) == 0){
                        /* Error decompressing string */
                        fprintf(stderr,"ERROR : Couldn't decompress string\n");
                        break;
                    }
                }
                else {
                    fseek(fd,size,SEEK_CUR);
                }
                free(c);
                if(args.full)
                    debug_print("DEBUG: str_enc\tvalue: %s\n",key->str);
                debug_print("DEBUG: str_enc() return\n");
                return key;
            default:
                debug_print("DEBUG: str_enc\tcase default\n");
                key->str = malloc(sizeof(char)+1+SPACE_FOR_NULL);
                key->str[0] = ' ';
                key->str[1] = '\0';
                if(args.full)
                    debug_print("DEBUG: str_enc\tvalue: %s\n",key->str);
                return key;
        }
    } else {
        key->str = malloc(sizeof(char) * key->size + SPACE_FOR_NULL);
        if(key->str == NULL){
            /* 
                If space cannot be allocated we want to fast forward the location in the file
                so that the key in its entirety is skipped. Otherwise it will tumble out of
                control grabbing incorrect data
            */
            debug_print("ERROR : Could not allocate space of size %llu\n",key->size);
            free(key);
            fseek(fd,key->size,SEEK_CUR);
            return NULL;
        }
        memset(key->str,'\0',key->size+1);
        if(key->size > 0)
            fread(key->str,key->size,1,fd);
        if(ferror(fd)){
            fprintf(stderr,"ERROR : Failed to read %llu bytes\n",key->size);
            free(key->str);
            free(key);
            return NULL;
        }
        key->size += STR_OH;
        if(args.full)
            debug_print("DEBUG: str_enc\tvalue: %s\n",key->str);
        return key;
    }
    /* WE SHOULDN'T GET HERE */
    return NULL;
}

static struct KI* list_enc(FILE *fd){
    /* 
        length encoding to determine number of strings in list
        then size of each string is found using string encoding 
    */
    char *str = NULL;
    struct KI *tmp, *key;
    unsigned long long i, lsize = 0;
    unsigned char buffer[BUFFERSIZE];
    memset(buffer,0x00,BUFFERSIZE);
    fread(buffer,1,1,fd);
    lsize = get_length(buffer,fd);
    key = str_enc(fd);
    debug_print("DEBUG: list_enc()\n");
    for(i=1;i<lsize;i++){
        tmp = str_enc(fd);
        key->size += tmp->size + 48;
        if(args.full){
            if(key->str == NULL){
                key->str = malloc(sizeof(char)*key->size+SPACE_FOR_NULL);
                memset(key->str,'\0',key->size+1);
                memcpy(key->str,tmp->str,key->size);
            }
            else{
                asprintf(&str,"%s, %s",key->str,tmp->str);
                free(key->str);
                key->str = str;
                str = NULL;
            }
        }
        free(tmp->str);
        free(tmp);
    }
    key->size += LIST_OH;
    return key;
}

static struct KI* set_enc(FILE *fd){
    /* same as list */
    return list_enc(fd);
}

static struct KI* sset_enc(FILE *fd){
    /* 
        Sorted Set:
        str_enc() to get name
        get_length() to get num of bytes to represent "score"

        now uses raw double format potentially instead of float
    */
    char *str = NULL;
    struct KI *key = NULL, *ktmp = NULL;
    unsigned char buffer[BUFFERSIZE], *bytelen = NULL;
    unsigned long long i, num = 0, score = 0, b64 = 0;
#if 0
    fseek(fd, -1, SEEK_CUR);
    fread(buffer,1,1,fd);
    if(buffer[0] == 0x5){
        b64 = 1;
        debug_print("DEBUG: 64 bit flag!\n");
    }
#endif
    fread(buffer,1,1,fd);
    debug_print("DEBUG: NEXT BYTE BEFORE NUM: %x\n", buffer[0]);
    num = get_length(buffer,fd);
    key = create_KI();
    debug_print("DEBUG: sset_enc() num : %llu\n",num);
    for(i=0;i<num;i++){
        ktmp = str_enc(fd);
        fread(buffer,1,1,fd);
        debug_print("sset_enc score byte: %x\n",buffer[0]);
#if 0
        if(b64)
            score = 8;
        else
            score = 4;
#endif
        #if 1
        score = get_length(buffer,fd);
        debug_print("sset_enc score : %llu\n",score);
        if(score == 253){
        /*
         * NaN
         */
        }else if(score == 254){
        /* 
         * INF
         */
        }else if(score == 255){
        /* 
         * -INF 
         */
        }else{
            bytelen = malloc(sizeof(char)*score+SPACE_FOR_NULL);
            memset(bytelen,'\0',score);
            fread(bytelen,1,score,fd);
        }
        #endif
        if(args.full){
            if(key->str == NULL)
                asprintf(&key->str,"%s > %llu",ktmp->str,score);
            else{
                asprintf(&str,"%s, %s > %llu",key->str,ktmp->str,score);
                free(key->str);
                key->str = str;
                str = NULL;
            }
        }
        key->size += ktmp->size + DICT_OH + score;
        free(bytelen);
        free(ktmp->str);
        free(ktmp);
        score = 0;
    }
    key->size += SSET_OH;
    return key;
}

static struct KI* sset64_enc(FILE *fd){
     char *str = NULL;
    struct KI *key = NULL, *ktmp = NULL;
    unsigned char buffer[BUFFERSIZE];
    unsigned long long i, num = 0, score = 0, b64 = 0;
    fread(buffer,1,1,fd);
    debug_print("DEBUG: NEXT BYTE BEFORE NUM: %x\n", buffer[0]);
    num = get_length(buffer,fd);
    key = create_KI();
    debug_print("DEBUG: sset_enc() num : %llu\n",num);
    for(i=0;i<num;i++){
        ktmp = str_enc(fd);
        fread(&score,1,8,fd);
        debug_print("sset_enc score : %llu\n",score);
        if(score == 253){
        /*
         * NaN
         */
        }else if(score == 254){
        /* 
         * INF
         */
        }else if(score == 255){
        /* 
         * -INF 
         */
        }
        if(args.full){
            if(key->str == NULL)
                asprintf(&key->str,"%s > %llu",ktmp->str,score);
            else{
                asprintf(&str,"%s, %s > %llu",key->str,ktmp->str,score);
                free(key->str);
                key->str = str;
                str = NULL;
            }
        }
        key->size += ktmp->size + DICT_OH + score;
        free(ktmp->str);
        free(ktmp);
        score = 0;
    }
    key->size += SSET_OH;
   
    return key;
}

static struct KI* hash_enc(FILE *fd){
    /* 
        size of hash is read using length encoding
        2 strings are read (field => value)
        Redis hashes are defined in dict
    */
    struct KI *tmp, *key = NULL;
    char *str = NULL;
    int rc = 0;
    unsigned char buffer[BUFFERSIZE];
    unsigned long long i, hsize = 0;
    key = create_KI();
    memset(buffer,0x00,BUFFERSIZE);
    fread(buffer,1,1,fd);
    hsize = get_length(buffer,fd);  
    debug_print("DEBUG: hash_enc()\n");
    for(i=0;i<hsize;i++){
        tmp = str_enc(fd);
        if(tmp != NULL)
            key->size += tmp->size + 24;
        if(args.full){
            if(key->str == NULL){
                rc = asprintf(&key->str,"%s =>",tmp->str);
            } else {
                rc = asprintf(&str,"%s, %s =>",key->str,tmp->str);
                free(key->str);
                key->str = str;
                str = NULL;
            }
            if(rc < 1)
                fprintf(stderr,"ERROR : asprintf() failed\n");
        }
        rc = 0;
        free(tmp);
        tmp = str_enc(fd);
        if(tmp != NULL)
            key->size += tmp->size + 24;
        if(args.full){
            rc = asprintf(&str,"%s %s",key->str, tmp->str);
            if(rc < 1){
                fprintf(stderr,"ERROR :  asprintf() failed\n");
            }
            free(key->str);
            key->str = str;
        }
        str = NULL;
        free(tmp->str);
        free(tmp);
    }
    /* Hash ROBJ pointer/dict overhead space */
    key->size += (56 + 32) * 6;
    return key;
}

static struct KI *mod_enc(FILE *fd){
    return NULL;
}

static struct KI* zm_enc(FILE *fd){
    /* allegedly deprecated... */
    return 0;
}

static struct KI* zl_enc(FILE *fd){
    /*
        zlbytes: 4 byte uint of total zip list size
        zltail : 4 byte uint in LITTLE endian of offset to tail
        zllen  : 2 byte uint in LITTLE endian of num of entries
        entry  : element in zip list
            length-prev-entry 
            special-flag
            raw-bytes-of-entry
        zlend  : 0xFF
        get_length
        parse using above format
    */
    struct KI *ktmp = NULL, *key = NULL;
    char *tmp = NULL;
    size_t tmpsize = 0;
    unsigned long offset = 10, keyoff = 0;
    ktmp = str_enc(fd);
    key = create_KI();
    key->size = ktmp->size;
    debug_print("DEBUG: zl_enc() size of key = %llu\n",key->size);
    if(args.full){
        key->str = malloc(key->size);
        memset(key->str,' ',key->size);
        key->str[key->size-1] = '\0';
        while((unsigned char)ktmp->str[offset] != 0xFF && offset < ktmp->size){
            tmp = get_zl_entry(ktmp->str+offset,&offset);
            debug_print("DEBUG: zl_enc() keyoff = %lu\n",keyoff);
            if(tmp == NULL) {
                continue;
            }
            /*else if(key->str == NULL){
                asprintf(&key->str,"%s",tmp);
            }*/
            else{
                #if 0
                asprintf(&str,"%s, %s",key->str,tmp);
                free(key->str);
                key->str = str;
                str = NULL;
                #endif
                tmpsize = strlen(tmp);
                memcpy(key->str+keyoff,tmp,tmpsize);
                keyoff += (unsigned)tmpsize;
                free(tmp);
            }
        }
    }
    free(ktmp->str);
    free(ktmp);
    return key;
}

static struct KI* is_enc(FILE *fd){
   /*
        after string encoding to get full size...
        first 4 bytes are encoding (2,4,8)
        next 4 bytes is length of contents
        contents
    */ 
    char *str = NULL;
    struct KI *tmp = NULL, *key = NULL;
    uint32_t num, type = 0, offset = 0;
    unsigned long size;
    debug_print("DEBUG: is_enc()\n");
    tmp = str_enc(fd);
    if(tmp == NULL)
        return NULL;
    size = tmp->size;
    memcpy(&type,tmp->str,4);
    memcpy(&num,tmp->str+4,4);
    offset+=8;
    key = create_KI();
    key->size = size;
    if(args.full){
        while(offset < size){
            if(type==0x2){
                uint16_t i = 0;
                memcpy(&i,tmp->str+offset,2);
                if(key->str == NULL)
                    asprintf(&key->str,"%u",i);
                else{ 
                    asprintf(&str,"%s, %u",key->str,i);
                    free(key->str);
                    key->str = str;
                    str = NULL;
                }
                offset+=2;
            } else if(type == 0x4){
                uint32_t i = 0;
                memcpy(&i,tmp->str+offset,4);
                if(key->str == NULL)
                    asprintf(&key->str,"%u",i);
                else{
                    asprintf(&str,"%s, %u",key->str,i);
                    free(key->str);
                    key->str = str;
                    str = NULL;
                }
                offset+=4;
            } else if(type == 0x8){
                uint64_t i = 0;
                memcpy(&i,tmp->str+offset,8);
                if(key->str == NULL)
                    asprintf(&key->str,"%" PRIu64 ,i);
                else{
                    asprintf(&key->str,"%s, %" PRIu64 ,key->str,i);
                    free(key->str);
                    key->str = str;
                    str = NULL;
                }
                offset+=8;
            }
        }
    }
    free(tmp->str);
    free(tmp);
    return key;
}

static struct KI* hmzl_enc(FILE *fd){
    /*
        Hash Map as a Ziplist
        Get entire value size using String Encoding
        Make sure the number of entries, num, is divisible by 2
        Get a Ziplist entry twice per iteration as the field =>value
    */
    char *tmp = NULL;
    size_t tmpsize = 0;
    struct KI *ktmp = NULL, *key = NULL;
    uint16_t i, num = 0;
    uint32_t zlbytes = 0, tail = 0;
    unsigned long offset = 0, keyoff = 0;
    debug_print("DEBUG: hmzl_enc()\n");
    ktmp = str_enc(fd);
    if(ktmp == NULL)
        return NULL; 
    memcpy(&zlbytes,ktmp->str,4);
    memcpy(&tail,ktmp->str+4,4);
    memcpy(&num,ktmp->str+8,2);
    offset += 10;
    if(num%2)
        fprintf(stderr,"ERROR : Don't make no sense\n");
    key = create_KI();
    key->size = zlbytes;
    /* num (number of hash values) * 6 (characters for formatting like =>) */
    key->str = malloc(key->size+(6*num)+1);
    memset(key->str,' ',(key->size+(6*num/2)));
    key->str[key->size+(6*num)] = '\0';
    if(key->str == NULL){
        debug_print("DEBUG: ERROR: hmzl_enc() Could not create space for key->str, size = %llu\n",key->size);
        return NULL;
    }
    debug_print("DEBUG: hmzl_enc() key->str allocated size : %llu\n",key->size);
    if(args.full){
        for(i=0;i<(num/2);i++){
            /* need to do null check before strlen! zl_entry can return NULL */
            tmp = get_zl_entry(ktmp->str+offset,&offset);
            if(tmp != NULL){
                tmpsize = strlen(tmp);
                memcpy(key->str+keyoff,tmp,tmpsize);
                keyoff += (unsigned)tmpsize;
                free(tmp);
                key->str[keyoff] = ' ';
                key->str[keyoff+1] = '=';
                key->str[keyoff+2] = '>';
                key->str[keyoff+3] = ' ';
                keyoff+=4;
            }
            tmp = get_zl_entry(ktmp->str+offset,&offset);
            if(tmp == NULL){
                keyoff++;
            }else{
                tmpsize = strlen(tmp);
                memcpy(key->str+keyoff,tmp,tmpsize);
                keyoff += (unsigned)tmpsize;
                free(tmp);
            }
            if(i != ((num/2) - 1)){
                key->str[keyoff] = ',';
                key->str[keyoff+1] = ' ';
                keyoff+=2;
            }
            debug_print("DEBUG: hmzl_enc() key->str value = %s\n\tkeyoff = %lu\n",key->str,keyoff);
        }
    }
    free(ktmp->str);
    free(ktmp);
    return key;
}

static struct KI* sszl_enc(FILE *fd){
    /*
        Sorted Set as a Ziplist
        Similar to hmzl above
    */
    char *tmp = NULL;
    size_t tmpsize = 0;
    struct KI *key = NULL, *ktmp = NULL;
    uint16_t i, num = 0;
    unsigned long offset = 10, keyoff = 0;
    debug_print("DEBUG: sszl_enc()\n");
    ktmp = str_enc(fd);
    memcpy(&num,ktmp->str+8,2);
    if(num%2){
        fprintf(stderr,"ERROR : Odd number of entries for SSZL which should not occur!\n");
    }
    key = create_KI();
    key->size = ktmp->size;
    key->str = malloc(key->size+(num*3*8)+1);
    /* 8 is for the size of an integer since scores are numbers those converted to ascii is going to need space */
    memset(key->str,' ',(key->size+(num*3*8)));
    key->str[key->size+(num*3)] = '\0';
    debug_print("DEBUG: sszl_enc() size of key = %llu\n",key->size);
    if(args.full){
        for(i=0;i<(num);i++){
            tmp = get_zl_entry(ktmp->str+offset,&offset);
            if(tmp == NULL){
                keyoff++;
            }else{
                tmpsize = strlen(tmp);
                memcpy(key->str+keyoff,tmp,tmpsize);
                keyoff += (unsigned)tmpsize;
            }
            if(i != (num-1)){
                key->str[keyoff] = ',';
                keyoff+=2;
            }else{
                key->str[keyoff] = '\0';
            } 
            free(tmp);
            debug_print("DEBUG: sszl_enc() key->str value = %s\n\tkeyoff = %lu\n",key->str,keyoff);
        }
    }
    free(ktmp->str);
    free(ktmp);
    return key;
}

static struct KI* ql_enc(FILE *fd){
    /* 
        Quicklist is a linked list of ziplists.
        Read number of entries in list with get_length()
        Iterate over list, every entry is a ziplist.
    */
    char *str = NULL;
    int i;
    struct KI *key = NULL, *ktmp;
    unsigned char buffer[BUFFERSIZE];
    unsigned long long num = 0;
    debug_print("DEBUG: ql_enc()\n");
    fread(buffer,1,1,fd);
    num = get_length(buffer,fd);
    key = create_KI();
    for(i = 0; i < num; i++){
        ktmp = zl_enc(fd);
        if(key->str == NULL){
            asprintf(&key->str,"%s",ktmp->str);
        } else {
            asprintf(&str,"%s | %s",key->str,ktmp->str);
            free(key->str);
            key->str = str;
            str = NULL;
        }
        key->size += QI_OH;
        free(ktmp->str);
        free(ktmp);
    }
    key->size += QL_OH;
    return key;
}

/*  End Encoding Functions  */

void print_key_info(struct KI *name, struct KI *value, uint8_t type, unsigned long exp, FILE *fo){
        if(name == NULL){
            fprintf(stderr,"ERROR : Could not get key name!\n");
            return;
        } else if (value == NULL) {
            fprintf(stderr,"ERROR : Could not get key value!\n");
            return;
        }
        fprintf(fo,"Key  : %s\n",name->str);
        switch(type){
            case 0:  fprintf(fo,"Type : String\n"); break;
            case 1:  fprintf(fo,"Type : List\n"); break;
            case 2:  fprintf(fo,"Type : Set\n"); break;
            case 3:  fprintf(fo,"Type : Sorted set\n"); break;
            case 4:  fprintf(fo,"Type : Hash\n"); break;
            case 9:  fprintf(fo,"Type : Zipmap\n"); break;
            case 10: fprintf(fo,"Type : Ziplist\n"); break;
            case 11: fprintf(fo,"Type : Intset\n"); break;
            case 12: fprintf(fo,"Type : Sorted set in ziplist\n"); break;
            case 13: fprintf(fo,"Type : Hashmap in ziplist\n"); break;
            case 14: fprintf(fo,"Type : Quicklist\n"); break;
            default: fprintf(fo,"Type : N/A\n"); 
        }
        fprintf(fo,"Size : %llu\n",(name->size+value->size)+ROBJ_OH); 
        fprintf(fo,"Exp  : %lu\n",exp);
        if(args.full){
            fprintf(fo,"Value: %s\n",value->str);
        }
        fprintf(fo,"\n");
}

int parse_args(int argc, char **argv){
    int rc = 0;
    /* 
        Parse Args
            rdb file
            out file
            silent/format
    */ 
    if(argc < 3 || argc > 5){
        fprintf(stderr,"ERROR : Incorrect number of arguments supplied.\n");
        print_usage;
        rc = 1;
    }else if(argc == 4 && argv[3][0] == 's'){
        /* silent */
        debug_print("DEBUG : Silent mode activated %s\n",argv[3]);
        args.noisy = 0;
    }else if(argc == 4 && argv[3][0] == 'f'){
        debug_print("DEBUG : Full output format %s\n",argv[3]);
        args.full = 1;
    }else if(argc == 4){
        fprintf(stderr,"ERROR : Bad third argument passed. Got %s\n",argv[3]);
        print_usage;
        rc = 1;
    }else if(argc == 5){
        if((argv[3][0] == 's' && argv[4][0] == 'f') || (argv[3][0] == 'f' && argv[4][0] == 's')){
            args.noisy = 0;
            args.full  = 1;
        }
        else{
            fprintf(stderr,"ERROR : Bad arguments passed. Got %s %s\n",argv[3],argv[4]);
            print_usage;
            rc = 1;
        }
    }
    return rc;
}

int check_magic(FILE *fd){
    unsigned char magic[5] = {0x52,0x45,0x44,0x49,0x53};
    char buffer[BUFFERSIZE];
    int i, rc = 0;
    fread(buffer,1,5,fd);
    if(ferror(fd)){
        fprintf(stderr,"ERROR : Failed to read 5 bytes from file to check magic!\n");
        rc = 2;
    }
    if(args.noisy){
        fprintf(stdout,"Check magic number ... 0x");
        for(i = 0; i < 5; i++) fprintf(stdout,"%.2x",buffer[i]);
    }
    if(memcmp(buffer,magic,5)){
        fprintf(stdout,"%17s\n","[FAIL]");
        fprintf(stderr,"ERROR : This is not a Redis RDB file!\n");
        rc = 3;
    } else if(args.noisy){
        fprintf(stdout,"%17s\n","[OK]");
    }
    return rc;
}

int check_rdb_version(FILE *fd){
    unsigned char RDB3[4] = {0x30,0x30,0x30,0x37};
    unsigned char RDB4[4] = {0x30,0x30,0x30,0x38};
    char buffer[BUFFERSIZE];
    int i, rc = 0;
    fread(buffer,1,4,fd);
    if(ferror(fd)){
        fprintf(stderr,"ERROR : Failed to read 4 bytes to check RDB version\n");
        rc = 2;
    }
    if(args.noisy){
        fprintf(stdout,"Check RDB version  ... 0x");
        for(i = 0; i < 4; i++) fprintf(stdout,"%.2x",buffer[i]);
    }
    if(memcmp(buffer,RDB3,4) != 0 || memcmp(buffer,RDB4,4) != 0){
        fprintf(stdout,"%19s\n","[FAIL]");
        fprintf(stderr,"ERROR : Incorrect RDB Version\n");
        rc = 3;
    } else if(args.noisy){
        fprintf(stdout,"%19s\n","[OK]");
    }
    return rc;
}
/* 
    Main
    Where the magic starts.
*/
int main(int argc, char **argv){
    clock_t begin, end;
    FILE *fd = NULL, *fo = NULL;
    int i, rc = 0;
    long pos, sz = 0, cur = 0, per  = 0;
    uint8_t type;
    uint64_t rdbtime = 0, exp = 0;
    struct KI* (*fptr[13])(FILE*) = {&str_enc, &list_enc, &set_enc, &sset_enc, 
                                    &hash_enc, &sset64_enc, &mod_enc, &zm_enc, 
                                    &zl_enc, &is_enc, &sszl_enc, &hmzl_enc, &ql_enc};
    struct KI *name = NULL, *value = NULL, *big = NULL;
    unsigned char buffer[BUFFERSIZE];
    unsigned long keycount = 0, keyper[11];
    args.noisy = 1;
    args.full  = 0;
    aux.x = 0;
    rc = parse_args(argc, argv);
    if(rc != 0) 
        goto end;
    fd = fopen(argv[1],"rb");
    if(fd == NULL){
        fprintf(stderr,"ERROR : Could not open file %s for binary read!\n",argv[1]);
        rc = 2;
        goto end;
    }
    fo = fopen(argv[2],"w+");
    if(fo == NULL){
        fprintf(stderr,"ERROR ; Could not open file %s for write!\n",argv[2]);
        rc = 2;
        goto end;
    }
    if(args.noisy){
        fprintf(stdout,"Redis RDB Dump Read\n");
        fprintf(stdout,"RDB File : %s\n",argv[1]);
        fprintf(stdout,"Out File : %s\n",argv[2]);
    /* Get file size for cool progress bar */
        fseek(fd, 0L, SEEK_END);
        sz = ftell(fd);
        rewind(fd);
    }
    /* Look for Redis Magic Number */
    rc = check_magic(fd);
    if(rc != 0)
        goto end;
    memset(buffer,'\0',BUFFERSIZE);
    /* Check RDB version. Currently we only support 0x30303037 */
    rc = check_rdb_version(fd);
    fprintf(stdout,"Redis RDB file verification complete.\nGetting Redis RDB info now...\n");
    for(i=0; i<12; i++)
        keyper[i] = 0;
    memset(buffer,'\0',BUFFERSIZE);
    big = create_KI();
    begin = clock();
    /*  Switch statement reads single byte to determine what it is
            FA : AUX?? Info keys before DB selected (redis version, options, etc) 
            FC : Expire in milliseconds
            FD : Expire in seconds
            FE : Select DB (we only use DB 0 so this doesn't always exist)
            FF : EOF 
    */
    while(fread(buffer,1,1,fd) != 0){
        if(feof(fd) || ferror(fd)){
            fprintf(stderr,"ERROR : fread() failure, quitting prematurely\n");
            rc = 2;
            goto end;
        }
        debug_print("TOP LEVEL BYTE  %x\n",buffer[0]);
        /* Progress bar because on big files it is difficult to tell if anything works */
        if(args.noisy && DEBUG == 0){
            pos = ftell(fd);
            per = (100*pos)/sz;
            if(per >= cur){
                fprintf(stdout,"\r[");
                for(i=0;i<(per/2);i++) fprintf(stdout,"#");
                for(i=(per/2);i<50;i++) fprintf(stdout," ");
                fprintf(stdout,"] %3ld%%",per);
                fflush(stdout);
                cur = per;
            }
        }
        switch(buffer[0]){
            case 0xFA:
                /* AUX is always string type */
                aux.x = 1;
                exp = 0;
                type = 0;
                break;
            case 0xFB:
                /* Resize DB */
                /* Not sure what this is but from what I can tell 
                    just length encoding to get stuff and then whatever */
                fread(buffer,1,1,fd);
                get_length(buffer,fd);
                fread(buffer,1,1,fd);
                get_length(buffer,fd);
                if(ferror(fd))
                    fprintf(stderr,"ERROR : Failed to read bytes for DB resizing\n");
                continue;
            /* 
                Expiration is set in 4 or 8 bytes after the 1 byte flag
                It needs byte flip (happens automatically by fread into int..?)
                If FC divide by 1000 to get seconds
                Then use the redis "ctime" which is the unix epoch stored during bgsave to
                    determine TTL from time of bgsave
             */
            case 0xFC:
                /* Next 8 bytes is expiration time */
                fread(&exp,1,8,fd);
                if(ferror(fd))
                    fprintf(stderr,"ERROR : Failed to read 8 bytes for expiration\n");
                exp = exp/1000;
                break;
            case 0xFD:
                /* Next 4 bytes is expiration time */
                fread(&exp,1,4,fd);
                if(ferror(fd))
                    fprintf(stderr,"ERROR : Failed to read 4 bytes for expiration\n");
                break;
            case 0xFE:
                /* Following byte is the DB */
                fread(buffer,1,1,fd);
                if(args.full)
                    fprintf(fo,"Database selected: %llu\n",get_length(buffer,fd));
                continue;
            case 0xFF:
                /* End of File */
                end = clock();
                int ttp = (end-begin)/CLOCKS_PER_SEC;
                int minute = 0;
                if (ttp > 60){
                    minute = ttp/60;
                    ttp = ttp%60;
                }
                fprintf(fo,"Total number of keys: %lu\n",keycount);
                fprintf(fo,"Largest key: %s with size %llu bytes\n",big->str,big->size);
                if(args.noisy){
                    fprintf(stdout,"\r[");
                    for(i=0;i<50;i++) fprintf(stdout,"#");
                    fprintf(stdout,"] 100%%\n");
                    fprintf(stdout,"Time to process file: %d:%.2d\n",minute,ttp);
                    fprintf(stdout,"Total number of keys: %lu\n",keycount);
                    fprintf(stdout,"Distribution:\n");
                    fprintf(stdout,"+++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                    fprintf(stdout,"+ Key Type + Number of Keys + Percentage of Total +\n");
                    fprintf(stdout,"+  String  +  %12lu  + %11.2f%%        +\n",keyper[0],(((float)keyper[0]*100)/(float)keycount));
                    fprintf(stdout,"+   List   +  %12lu  + %11.2f%%        +\n",keyper[1],(((float)keyper[1]*100)/(float)keycount));
                    fprintf(stdout,"+   Set    +  %12lu  + %11.2f%%        +\n",keyper[2],(((float)keyper[2]*100)/(float)keycount));
                    fprintf(stdout,"+Sorted Set+  %12lu  + %11.2f%%        +\n",keyper[3],(((float)keyper[3]*100)/(float)keycount));
                    fprintf(stdout,"+   Hash   +  %12lu  + %11.2f%%        +\n",keyper[4],(((float)keyper[4]*100)/(float)keycount));
                    fprintf(stdout,"+  Zipmap  +  %12lu  + %11.2f%%        +\n",keyper[5],(((float)keyper[5]*100)/(float)keycount));
                    fprintf(stdout,"+ Ziplist  +  %12lu  + %11.2f%%        +\n",keyper[6],(((float)keyper[6]*100)/(float)keycount));
                    fprintf(stdout,"+  Intset  +  %12lu  + %11.2f%%        +\n",keyper[7],(((float)keyper[7]*100)/(float)keycount));
                    fprintf(stdout,"+   SSZL   +  %12lu  + %11.2f%%        +\n",keyper[8],(((float)keyper[8]*100)/(float)keycount));
                    fprintf(stdout,"+   HMZL   +  %12lu  + %11.2f%%        +\n",keyper[9],(((float)keyper[9]*100)/(float)keycount));
                    fprintf(stdout,"+Quicklist +  %12lu  + %11.2f%%        +\n",keyper[10],(((float)keyper[10]*100)/(float)keycount));
                    fprintf(stdout,"+++++++++++++++++++++++++++++++++++++++++++++++++++\n");
                    fprintf(stdout,"Largest key: %s with size %llu bytes\n",big->str,big->size);
                    fprintf(stdout,"Dumpread complete.\n");
                }
                goto end;
            default:
                /* Key with no expiration so this byte is the type */
                exp=0;
                type=buffer[0];
                break;
        }
        /* Next byte should be type */
        if(buffer[0] == 0xFC || buffer[0] == 0xFD){
            fread(&type,1,1,fd);
            if(ferror(fd))
                fprintf(stderr,"ERROR : Failed to get byte for type\n");
        }
        debug_print("DEBUG: type : %x\n",type);
        if(type > 14 || type < 0) continue;
        /* Next byte sequence is the key name which is str_enc */
        memset(buffer,0x00,BUFFERSIZE);
        name = str_enc(fd);
        if(type < 9){
            value = (*fptr[type])(fd);
            keyper[type]++;
        } 
        else{
            value = (*fptr[type-2])(fd);
            keyper[type-4]++;
        }
        /* The value of the "ctime" key is used for base to get expiration */
        if((name->str != NULL) && type == 0 && strncmp(name->str,"ctime",5) == 0){
            rdbtime = strtou64(value->str);
        }
        if(exp > 0){
            exp = exp - rdbtime;
            name->size += EXP_OH;
        }
        print_key_info(name,value, type, exp, fo);
        if((name->size + value->size) > big->size){
            if(name->str != NULL){
                unsigned int sss = strlen(name->str);
                free(big->str);
                big->str = malloc(sss+1);
                memcpy(big->str,name->str,sss);
                big->str[sss] = '\0';
                big->size = name->size + value->size;
            }
        }
        /* 
            Free up memory to keep impact down
            Reinitialize variables for next key
        */
        free(name->str);
        free(name);
        free(value->str);
        free(value);
        type = -1;
        exp = 0;
        aux.x = 0;
        keycount++;
    }
end:
    if(fd != NULL)
        fclose(fd);
    if(fo != NULL)
        fclose(fo);
    return rc;
}
