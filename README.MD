# SFVFS is single file virtual file system for Java #

SFVFS supports files and directories. You can create, delete, copy or move any file or directory. Also you can read, write or append files data. SFVFS doesn't support multithreading, has no corruption protection, no file attributes, no random access of files data, no hard- sym- links. SFVFS focus on minimising RAM and disk usage. SFVFS has special compact operation to reduce container size to actual used size. SFVFS is similar to ext2 in a way it's designed.
 
Block size and amount of maximum blocks has to be specified on SFVFS creation. There is no way to change it after. The amount of (4 * maximum blocks) will be allocated in file and in memory at creation time. Another (4 * maximum blocks) memory will be used during compaction process. 

There is no file size limit. Append operations has O(1) complexity (as well as read and write). Empty file takes single block, but file will data will take at least two. No random access. Input and output memory overhead are O(1) (closed to block size). 

There is no limit on items in directory, but it's performance connected with SFVFS block size and length of item name. On 4kb block size it ok to have about 20k items with short names, but for more items performance degrade significantly. For indexed and relative small directory operations complexity is O(1). Any directory will take at least 2 blocks. Maximum name length can be specified when SFVFS created (and it should be at least twice less than block size).

Block allocation usually takes O(1) but can degrade to O(blocks/block size) if there is not much blocks left. Block allocator will use some (relative small) memory for caches (can be configured during SFVFS creation time). Block allocator tries to use existing empty blocks instead of allocating new memory.

Compaction process will take O(blocks) time and (4 * maximum blocks) memory to complete. Compaction process will leave data file size close to size of entities stored inside.

There is a hard limit on max blocks size (up to 4 * 1024 * 1024), blocks pointers are signed int4 and pointers inside file are signed int8, besides that there is no limit on container size. Having 4 * 1024 * 1024 blocks with a 4kb block will give up to 4gb data size inside container and requires at least 17mb extra ram (without allocation cache and input/output stream buffers).

The is no corruption protection and recovery procedure, but SFVFS designed to leave data in least ambitious state if container or streams were not closed properly. No proper shutdown may leave unreachable blocks, wrong directory size, lost directory entities or lost not flushed files data (size of a block) and any other data left un flushed in RAF in SFVFS opened in "rw" mode.

Move operation will take O(1) and copy O(blocks to copy).

## Usage ##

SFVFS uses standart Java File System API (with limitations).

`final Path sfvfsRoot = Paths.get(URI.create("sfvfs:/path/to/container/file.dat:/"));`

Examples could be found in `SFVFSFilesystemRealDataTest.java`.

TBD: configure container params via url

## Implementation ##

### Blocks ###

SFVFS use RAF to store blocks data. First part of file contains logical -> physical address mapping (array where index is logical and value is physical). The rest of data are blocks. Blocks are divided into groups (group size equals to block size) and every first block in group contains group allocation information (1b per block). 

Some groups with free blocks are cached in ram. When cache is empty allocator goes through already allocated group first and creates new empty groups after (if still necessary). Logical addresses are also stored into cache. To populate caches allocator goes through source in circular order (starts from previously remembered position). When block deallocated it goes to cache (if space left there).

When SFVFS created logical -> physical address mapped almost directly, but during compaction operation physical addresses may change. Compaction process move blocks form end of the file to the beginning of the file until possible. When block moved address mapping also updated. All empty groups at the end of fill will be released during compaction.

### Files ###

Every file in SFVFS has it's own Inode. Inode is regular block, but it contains pointers to file data along with file flags, it's size, pointer to next Inode and pointer to last file Inode (used for appending). File Inodes are chained.

Read operation goes through all data blocks in all Inodes sequentially. Current Inode block and data block are cached into memory.

Write operation takes last Inode from root Inode and append data to last data block. Current data block and Inode block are cached into memory as well. When data block fully written it flushes on RAF as well as corresponding Inode and file size also updated. Flush operation could be triggered manually and triggers on close. 

When clear or delete Inode releases it's block sequentially.

### Directory ###

Every directory in SFVFS has it's own Directory block. Every directory entity has a name, flags and pointer to it's root Inode/Directory block.

Directory block contains directory flags and pointers to Entity lists (see below). Directory can work in 2 modes: plain and indexed. For plain directory there is only one pointer to Entity list, and for indexed the rest of root directory block contains pointers to Entity lists. Exact Entity lists calculated by name's SHA-256 hash code. Entire hash code range mapped to directory Entity lists pointers in directory root block. When plain directory size reach some threshold (defined on SFVFS creation) it converts into indexed dir.

Entity list block contains pointer to next entity list block and entity entries. Inside entry stored it's block address, flags, name length and name itself. Entries goes one by one inside list node. To add new entity process goes through Entity list blocks until it finds block with enough space (new entity list block will be created if needed). To remove entity entire Entity list block recreated without the entity (if this block has no more entities than it will be removed from blocks chain).
 
To list directory entities iterator goes through every existing Entity list. Entity list blocks also has it's size in root block - directory size is sum of this sizes.

### java.nio.file.spi.FileSystemProvider ###

High level directory API exposed through Java FS API (with limitation). SFVFS FileSystem implementation has link to data blocks and pointer to root directory (block address  = 1). The rest of implementation is trivial.

## Performance ##

Test directory contains small java sources files with couple of large archives: 4472 files, 85M in total.

Test case (warmup and test stage): 

- load all files into container (respecting dir structure) 
- copy them in container
- remove origin dir in container
- compact container
- find copied files one by one and verify its content against source
- close container

All test are made in "rw" mode. Max blocks 1024 * 1024, indexed dir threshold 40, no logging.

Tests made on Mac Pro 2,2 GHz Intel Core i7 16GB SSD.

### Tests run ###

- rw block: 1024 -Xmx10m: OOM
- rw block: 1024 -Xmx15m: time: ~20s filesize: 93M
- rw block: 1024 -Xmx30m: time: ~20s filesize: 93M
- rw block: 1024 -Xmx50m: time: ~19s filesize: 93M
- rw block: 1024 -Xmx500m: time: ~21s filesize: 93M
- rw block: 512: time: ~23s filesize: 88M
- rw block: 4096: time: ~13s filesize: 138M

### Profiled run ###

- Heap usage: Avg 40Mb Max 71Mb
- Total CPU: Avg 40% Max 65%
- GC: Avg 1.2ms Max 7.6ms

### Results ###

SFVFS itself does not depend on memory, it's working on very small  (15MB) and big (500MB) heap sizes with same performance. 
When SFVFS runs on "rw" mode it doesn't affect disk either (no fsync called until closing container).
But it's need to remember that OS caches RAF pages and if there is no ram left for it OS will read/write to disk more often.
The most impact goes to CPU. Besides IOUtils there is Input and Output streams in Hot Methods. Which is expected due to lack of read(byte[]) and write(byte[]) methods optimal implementation.

## Possible future improvements ##
 
- To increase overall performance best thing to do would be implementing read(byte[]) and write(byte[]) optimal.
- To increase durability the best thing would be to create a journal of applied data (which would be written in append only mode and fsync flushes for every transaction).
- Another durability improvement would be creating data repair process.
- To increase performance in concurrent environment is possible to implement "read many, write one locking" with relative small changes. Also some processes (block allocation preparation, compact) could be moved to separate thread.
- Random access could be achieved by adding indirect Inodes.
- Adding another layer into hash indexed directories would increase amount of possible files in dir significantly.
- Moving configuration settings from constants would be also great. 

## How to build ##

TBD