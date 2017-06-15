/* Michael Coates : mcoates@wayfair.com : @outerwear
    HOW TO RUN:
        prefix [filename] [optional:short]
    ARGUMENTS:
        [filename]      - dumpread output filename to analyze
        [optional:short] - changes output format to be short hand (comma delimited)
    RETURN CODES:
        0 - Success!
        1 - Something bad
        2 - Can't initialize trie structure
    NOTES:
        Works with dumpread
        Format of my output
            KEY  : ...\n
            TYPE : ...\n
            VALUE: ...\n
            SIZE : ...\n
            EXPIR: ...\n
        Read X number of characters from KEY to get "prefix"
        Check second letter of TYPE to get TYPE quickly
        Skip VALUE as that isn't super important
        SIZE is just a number after, grab up to newline
        EXPIR is expiration and same as size (can be 0)
        Default behavior is to print to stdout so if you want to save this info redirect it
            to a file or something.
        Consider putting the while loop into its own function for cleanliness
        Requires the short dumpread output currently. TODO : make work with full which is 
            probably just uncomment the do while loop.
        I recommend piping output to a file if you want to save it. I don't do it default because 
            sometimes I just wanted everything in stdout.
*/

#include <ctype.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* 
    BUFSIZE is just the max size I grab from the dumpread output. It really isnt a special number.
    KEY_CHAR : number of max elements in trie (26 letters of alphabet, 10 numbers)
    START LETTER and NUMBER are the decimal values for ascii characters A and 0
    START SYM# is the offset to the Trie when compared to the decimal value of the char
*/
#define BUFSIZE         999
#define KEY_CHAR        66
#define START_LETTER    65
#define START_NUMBER    22
#define START_SYM1      -3
#define START_SYM2      6
#define START_SYM3      31

uint8_t pretty = 1;

/*
    Trie structure to hold all prefixes and some basic info on the keys
    Not sure if we have prefixes that use different types, it seems that usually a prefix is 
        associated with a single Redis data type like string. More on that later...
    ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789!"#$%&'()*+,-./:;<=>?@[\]^_`
*/
struct KT {
    uint8_t type; /* Redis data type */
    uint32_t num; /* Number of keys with this token */
    uint64_t size; /* Total size consumed by these keys */
    uint64_t avgttl; /* average ttl in seconds */
    uint64_t bigttl; /* largest ttl of all keys with prefix in seconds */
    struct KT *next[KEY_CHAR];
};

/*
    Initialize an empty node in the trie
*/
struct KT* create_KT(){
    struct KT *tmp = malloc(sizeof(struct KT));
    if(tmp == NULL) return NULL;
    memset(tmp->next,'\0',KEY_CHAR);
    tmp->num = 0;
    tmp->size = 0;
    tmp->avgttl = 0;
    tmp->bigttl = 0;
    return tmp;
}

/*
    Recursive method to display nice looking table of key prefix information
    That free(nn) is crucial for many reasons
*/
void print_full_analysis(struct KT *tr, char* name, int sz){
    char *nn;
    uint8_t i;
    if(name == NULL){
        name = malloc(sizeof(char));
        name[0] = '\0';
        sz = 1;
    }
    if(tr->num > 0){
        /* calculate percentage of keys from dump */
        if(pretty){
            if(tr->type == 1)      printf("| %-30.30s |    Hash    | %-16"PRIu32" | %-18"PRIu64" | %-21"PRIu64" | %-21"PRIu64" |\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 2) printf("| %-30.30s |    Set     | %-16"PRIu32" | %-18"PRIu64" | %-21"PRIu64" | %-21"PRIu64" |\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 3) printf("| %-30.30s |    List    | %-16"PRIu32" | %-18"PRIu64" | %-21"PRIu64" | %-21"PRIu64" |\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 4) printf("| %-30.30s |   Intset   | %-16"PRIu32" | %-18"PRIu64" | %-21"PRIu64" | %-21"PRIu64" |\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 5) printf("| %-30.30s | Sorted Set | %-16"PRIu32" | %-18"PRIu64" | %-21"PRIu64" | %-21"PRIu64" |\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 6) printf("| %-30.30s |   String   | %-16"PRIu32" | %-18"PRIu64" | %-21"PRIu64" | %-21"PRIu64" |\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 7) printf("| %-30.30s | Quicklist  | %-16"PRIu32" | %-18"PRIu64" | %-21"PRIu64" | %-21"PRIu64" |\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 8) printf("| %-30.30s |   Multi    | %-16"PRIu32" | %-18"PRIu64" | %-21"PRIu64" | %-21"PRIu64" |\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else                   printf("| %-30.30s |    N/A     | %-16"PRIu32" | %-18"PRIu64" | %-21"PRIu64" | %-21"PRIu64" |\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
        } else {
            if(tr->type == 1)      printf("%s,Hash,%" PRIu32 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 2) printf("%s,Set,%" PRIu32 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 3) printf("%s,List,%" PRIu32 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 4) printf("%s,Intset,%" PRIu32 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 5) printf("%s,Sorted Set,%" PRIu32 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 6) printf("%s,String,%" PRIu32 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 7) printf("%s,Quicklist,%" PRIu32 ",%" PRIu64",%" PRIu64 ",%" PRIu64"\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else if(tr->type == 8) printf("%s,Multi,%" PRIu32 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
            else                   printf("%s,N/A,%" PRIu32 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",name,tr->num,tr->size,(tr->avgttl/tr->num),tr->bigttl);
        }
    }
    for(i=0;i<KEY_CHAR;i++){
        if(tr->next[i] == NULL) continue;
        nn = malloc(sz+1);
        if(i>25 && i<=35)
            snprintf(nn,sz+1,"%s%c",name,(char)(START_NUMBER+i));
        else if(i>35 && i<=51)
            snprintf(nn,sz+1,"%s%c",name,(char)(START_SYM1+i));
        else if(i>51 && i<=59)
            snprintf(nn,sz+1,"%s%c",name,(char)(START_SYM2+i));
        else if(i>59)
            snprintf(nn,sz+1,"%s%c",name,(char)(START_SYM3+i));
        else
            snprintf(nn,sz+1,"%s%c",name,(char)(START_LETTER+i));
        print_full_analysis(tr->next[i],nn,sz+1);
        free(nn);
    }
}

/*
    MAIN where the works starts and ends
*/
int main(int argc, char *argv[]){
    if(argc < 2 || argc > 3){
        printf("Usage: %s [dump out file] [optional:short]\n", argv[0]);
        return 1;
    }
    if(argc == 3 && strncmp(argv[2],"short",4) == 0){
        pretty = 0;
    } else if (argc == 3){
        printf("Unknown option passed: %s\n",argv[2]);
        printf("Usage: %s [dump out file] [optional:short]\n", argv[0]);
        return 1;
    }
    struct KT *tmp, *nkt = NULL, *tr = NULL;
    uint8_t type = 0;
    uint32_t i = 0;
    uint64_t exp = 0;
    int upper;
    FILE *fd = fopen(argv[1],"r");
    char key[BUFSIZE];
    memset(key,'\0', (BUFSIZE));
    tr = create_KT();
    if(tr == NULL){
        printf("Could not initialize KT!!\n");
        return 2;
    }
    while(fgets(key,BUFSIZE,fd) != NULL){
        i = 0;
        tmp = tr;
        if(key[0] == 'K' && key[1] == 'e' && key[2] == 'y'){
            /* 
               Key name 
               Set i offset to 7 because rdb dump lists field as 'Key  : NAME'
               Loop until symbol is reached
             */
            for(i=7;i<BUFSIZE;i++){
                upper = toupper(key[i]);
                if((upper > 'Z') || (upper < '0') || ((upper > '9') && (upper < 'A')) || ((i == 16))){
                    tmp->num++;
                    /* get type */
                    memset(key,'\0', (BUFSIZE));
                    fgets(key,BUFSIZE,fd);
                    upper = toupper(key[8]);
                    if(tmp->type != 8){
                        switch(upper){
                            case 65: type = 1; break;
                            case 69: type = 2; break;
                            case 73: type = 3; break;
                            case 78: type = 4; break;
                            case 79: type = 5; break;
                            case 84: type = 6; break;
                            case 85: type = 7; break;
                            default: type = 0; break;
                        }
                        if(tmp->type != 0 && tmp->type != type) tmp->type = 8;
                        else tmp->type = type;
                    }
                    /* Skip the value */
                    memset(key,'\0', (BUFSIZE));
                    /*while(key[0] != 'S' &&
                            key[1] != 'i' &&
                            key[2] != 'z' &&
                            key[3] != 'e' ){
                        fgets(key,BUFSIZE,fd);
                    }*/
                    fgets(key,BUFSIZE,fd);
                    tmp->size += atol(key+7);
                    memset(key,'\0',BUFSIZE);
                    fgets(key,BUFSIZE,fd);
                    exp = atol(key+7);
                    if(exp > tmp->bigttl){
                        tmp->bigttl = exp;
                    }
                    tmp->avgttl += exp;
                    /* save biggest key */
                    break;
                }else{
                    /* Get trie offset for character */
                    if(upper > 'Z') upper -= START_SYM3; 
                    else if(upper < '0') upper -= START_SYM1;
                    else if((upper > '9') && (upper < 'A')) upper -= START_SYM2;
                    else if((upper >= '0') && (upper <= '9')) upper -= START_NUMBER;
                    else upper -= START_LETTER;
                    if(tmp->next[upper] == NULL){
                        nkt = create_KT();
                        if(nkt == NULL){
                            printf("Couldn't make a new KT for key name, continue to next key\n");
                            break;
                        }
                        tmp->next[upper] = nkt;
                        nkt = NULL;
                    }
                    tmp = tmp->next[upper];
                }
            }
        }
        else if(key[0] == 'T' &&
                key[1] == 'o' &&
                key[2] == 't' &&
                key[3] == 'a' &&
                key[4] == 'l'){
            

            /* +26 */
        }
        /* Else: Skip cause whatever */
        memset(key,'\0',BUFSIZE);
    }
    /* Print all key prefixes and their information */
    if(pretty){
        printf("|--------------------------------|------------|------------------|--------------------|-----------------------|-----------------------|\n");
        printf("|           Key Prefix           |    Type    |  Number of Keys  |    Size (Bytes)    | Average TTL (Seconds) | Largest TTL (Seconds) |\n");
        printf("|--------------------------------|------------|------------------|--------------------|-----------------------|-----------------------|\n");
        print_full_analysis(tr,0,0);
        printf("|--------------------------------|------------|------------------|--------------------|-----------------------|-----------------------|\n");
    } else {
        print_full_analysis(tr,0,0);
    }
    free(tr);
    fclose(fd);
    return 0;
}
