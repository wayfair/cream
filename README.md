# cream

**Cache Rules Everything Around Me**

The cache is a very important system that is necessary for a fast and reliable
customer experience. Here at Wayfair, we use Redis as a primary caching
solution, so we quickly found the need to monitor its internal state. Redis
itself provides a lot of useful info to give us an idea of what is going on but
sometimes we need to know more. Keyspace is an area that Redis doesn't provide a
simple, fast way to look at in a production environment, so we can utilize the
BGSAVE function and analyze the resulting RDB files to allows us to see what is
stored in these nodes. Parsing that data and extracting useful information from
it needs to be done often to provide reliable monitoring of our Redis clusters.

## Build

You will need `gcc` installed. Other than that it is a simple Makefile to be
invoked which will generate two executables.

```
make
```

If you want to compile with debug options (this will create very verbose
output!!) simply run this instead:

```
make debug
```

## Dumpread

Parses a Redis RDB file from a BGSAVE and outputs in a human-readable format.
It is heavily inspiried by https://github.com/sripathikrishnan/redis-rdb-tools,
but that Python implementation was too slow for our production needs. At
Wayfair, we have large Redis clusters with many millions, sometimes billions, of
keys per node. Writing this in C provided a significant performance improvement,
speeding up a single run from many hours to only minutes. Aside from a speed
perspective, memory consumption was greatly reduced as well.

Quick run-down of how it works: It reads the file byte-by-byte and, according to
Redis spec, each key type is started (and sometimes terminated) with a specific
byte value. This switch loop in `main` handles that and uses the function
pointer associated with the key type. I use the same LZF compression library
that Redis uses to make my life a lot easier and to guarantee correct
decompression. Key data is stored in two identical structs:

```c
struct KI {
    unsigned long size;
    char *str;
};
```

Where "size" is the size of the name or value being stored and "str" is the
data. The expiration is calculated by using the `ctime` key that is included in
Redis RDB file and subtracting the expiration integer data to see the exact TTL
from when the BGSAVE was done.

**NOTE**: This is verified to work with Redis 3.2 as that is what we use at 
Wayfair. I've had success using it with 2.8 but it isn't guaranteed and will 
require the RDB version check to be modified. In the future it would be nice to
add additional Redis version support as the newest stable version is just around
the corner. Also size is just a quick and dirty estimation (assuming 64 bit app 
on a 64 bit machine) but is close enough for our metrics.

### Sample Output

```
% ./dumpread dump.rdb dump.out
Redis RDB Dump Read
RDB File : dump.rdb
Out File : dump.out
Check magic number ... 0x5245444953             [OK]
Check RDB version  ... 0x30303037               [OK]
Redis RDB file verification complete.
Getting Redis RDB info now...
[##################################################] 100%
Time to process file: 4:07
Total number of keys: 16149270
Distribution:
+++++++++++++++++++++++++++++++++++++++++++++++++++
+ Key Type + Number of Keys + Percentage of Total +
+  String  +      10554800  +       65.36%        +
+   List   +             0  +        0.00%        +
+   Set    +        207936  +        1.29%        +
+Sorted Set+            85  +        0.00%        +
+   Hash   +       2168592  +       13.43%        +
+  Zipmap  +             0  +        0.00%        +
+ Ziplist  +             0  +        0.00%        +
+  Intset  +             0  +        0.00%        +
+   SSZL   +       2874632  +       17.80%        +
+   HMZL   +        343193  +        2.13%        +
+Quicklist +            32  +        0.00%        +
+++++++++++++++++++++++++++++++++++++++++++++++++++
Largest key: test:key21 with size 25730746 bytes
Dumpread complete.
```

4 minutes and 7 seconds to process 16,149,270 keys with 65.36% majority of type
string. These keys are of wildly varying size and the out file defined as 
dump.out can be viewed to see more info on each specific key. 
If we open up that out file we see:

```
Key  : ds:041734d4-7dc1-449f-83a1-adfbc445f363
Type : String
Size : 121
Exp  : 576063
```

That is a single key output, the file will contain all the keys and information.
If you add the 'full' argument to `dumpread` then it will also have a value 
field that will display the contents of the keys. Lists, sets, hashes, and 
ziplists will have each field/value separated with either a comma or =>. Size is
in bytes and Expiration is seconds left from the time the BGSAVE was run.

## Prefix

This is a utility to quickly sort key prefixes and get some cumulative data. 
Here at Wayfair we prepend our keys with a prefix that is designated by team, 
service, or other. It allows us to quickly determine a key's purpose (especially
if it is a generated hashed name) or who owns the key for quicker debugging and 
readability. Using Prefix we are able to then gather all the key prefixes and 
their information across all Redis clusters quickly and graph it to monitor. It 
has two types of output: normal and shorthand. The normal is best if you are 
reading it yourself and want a pretty table to view everything. Shorthand is 
used if you want to then store that data somewhere (i.e. Redis) to then graph 
it.

Prefix takes advantage of a Trie to quickly store and access all prefixes along
with keeping total memory usage down. All characters are passed through
`toupper()` to have a smaller memory footprint so it is assumed that the user
will not have keys that are prefixed with the same name but different case since
it would all be lumped into a single prefix. This application is especially
fast, typically taking only a few seconds to rip through a few million key out
file. Prefix will default send the resulting table to stdout so if you want to
save this information then you should redirect it to a file.

### Sample Output

```
% ./prefix keys.out
|--------------------------------|------------|------------------|--------------------|-----------------------|-----------------------|
|           Key Prefix           |    Type    |  Number of Keys  |    Size (Bytes)    | Average TTL (Seconds) | Largest TTL (Seconds) |
|--------------------------------|------------|------------------|--------------------|-----------------------|-----------------------|
| BROWSE                         |   String   | 14339            | 4657553            | 164439                | 164441                |
| DS                             |   String   | 1245248          | 150675008          | 576005                | 576126                |
| ME                             |   Multi    | 63951            | 7980517            | 594476                | 616479                |
|--------------------------------|------------|------------------|--------------------|-----------------------|-----------------------|
```

This is a clip of the output expected from the prefix app. The separators to
determine a prefix are special characters such as `[:,_]`. Prefix will also
convert all characters to uppercase to use up less space in the trie so the
output will reflect that conversion.
