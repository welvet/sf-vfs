package sfvfs.internal;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.Map;

import static sfvfs.utils.Preconditions.*;

/**
 * @author alexey.kutuzov
 */
public class DataBlocks {

    private static final int DEFAULT_BLOCK_SIZE = 4 * 1024;
    private static final int BLOCK_GROUPS_WITH_FREE_BLOCKS_CACHE_SIZE = 100;

    private final int blockSize;
    private final int groupSize;
    private final int blocksInGroup;
    private final int blockGroupsWithFreeBlocksCacheSize;
    private final RandomAccessFile dataFile;

    private final Map<Integer, BlockGroup> blockGroupsWithFreeBlocks = new HashMap<>();
    private int allocatedGroups;

    public DataBlocks(final File file) throws IOException {
        this(file, DEFAULT_BLOCK_SIZE, BLOCK_GROUPS_WITH_FREE_BLOCKS_CACHE_SIZE);
    }

    DataBlocks(final File file, final int blockSize, final int blockGroupsWithFreeBlocksCacheSize) throws IOException {
        checkArgument(blockSize > 0, "block size must be more than zero");
        checkArgument(((blockSize & -blockSize) == blockSize), "block size must be power of 2");
        checkNotNull(file, "file");

        this.dataFile = new RandomAccessFile(file, "rw");

        this.blockSize = blockSize;
        this.groupSize = blockSize * blockSize;
        this.blocksInGroup = blockSize;
        this.allocatedGroups = (int) (this.dataFile.length() / groupSize);
        this.blockGroupsWithFreeBlocksCacheSize = blockGroupsWithFreeBlocksCacheSize;
    }

    public Block allocateBlock() throws IOException {

        if (blockGroupsWithFreeBlocks.isEmpty()) {
            for (int i = 0; i < allocatedGroups; i++) {
                final BlockGroup blockGroup = getBlockGroup(i);

                if (blockGroup.hasFreeBlocks()) {
                    blockGroupsWithFreeBlocks.put(i, blockGroup);
                    if (blockGroupsWithFreeBlocks.size() >= blockGroupsWithFreeBlocksCacheSize) {
                        break;
                    }
                }
            }

            while (blockGroupsWithFreeBlocks.size() < blockGroupsWithFreeBlocksCacheSize) {
                final BlockGroup newGroup = new BlockGroup(allocatedGroups++);
                blockGroupsWithFreeBlocks.put(newGroup.id, newGroup);
            }
        }

        final BlockGroup blockGroup = blockGroupsWithFreeBlocks.values().iterator().next();
        try {
            return blockGroup.allocateBlock();
        } finally {
            if (!blockGroup.hasFreeBlocks()) {
                blockGroupsWithFreeBlocks.remove(blockGroup.id);
            }
        }

    }

    public void deallocateBlock(final int blockAddress) throws IOException {
        final BlockGroup blockGroup = getBlockGroup(blockAddress / groupSize);
        blockGroup.deallocateBlock((blockAddress % groupSize) / blockSize);
        if (blockGroupsWithFreeBlocks.size() < blockGroupsWithFreeBlocksCacheSize) {
            if (!blockGroupsWithFreeBlocks.containsKey(blockGroup.id)) {
                blockGroupsWithFreeBlocks.put(blockGroup.id, blockGroup);
            }
        }
    }

    public Block getBlock(final int blockAddress) {
        //no allocation check
        return new Block(blockAddress);
    }

    public int debugGetTotalBlocks() throws IOException {
        return allocatedGroups * blocksInGroup;
    }

    public int debugGetFreeBlocks() throws IOException {
        int result = 0;
        for (int i = 0; i < allocatedGroups; i++) {
            result += getBlockGroup(i).getFreeBlocks();
        }

        return result;
    }

    private BlockGroup getBlockGroup(final int id) throws IOException {
        final BlockGroup blockGroup = blockGroupsWithFreeBlocks.get(id);
        if (blockGroup != null) {
            return blockGroup;
        }

        return new BlockGroup(id);
    }

    private class BlockGroup {
        private final int id;
        private int freeBlocks = -1;
        private final byte[] cachedMetaBlockData;

        BlockGroup(final int id) throws IOException {
            this.id = id;

            if (dataFile.length() < id * groupSize) {
                dataFile.seek(id * groupSize);
                final byte[] emptyBytes = new byte[blockSize];
                for (int i = 0; i < blocksInGroup; i++) {
                    dataFile.write(emptyBytes);
                }

                cachedMetaBlockData = emptyBytes;
                freeBlocks = blocksInGroup - 1;
            } else {
                dataFile.seek(id * groupSize);
                final byte[] blockData = new byte[blockSize];
                dataFile.read(blockData);

                cachedMetaBlockData = blockData;
            }
        }

        boolean hasFreeBlocks() throws IOException {
            initFreeBlocksCounter();

            return freeBlocks > 0;
        }

        int getFreeBlocks() throws IOException {
            initFreeBlocksCounter();

            return freeBlocks;
        }

        Block allocateBlock() throws IOException {
            initFreeBlocksCounter();
            checkState(freeBlocks > 0, "no free blocks for " + id);

            for (int i = 1; i < blocksInGroup; i++) {
                final Flags.BlockGroupFlags currentBlockFlags = new Flags.BlockGroupFlags(cachedMetaBlockData[i]);

                if (!currentBlockFlags.isTaken()) {
                    currentBlockFlags.setTaken(true);

                    freeBlocks--;
                    updateBlockData(i, currentBlockFlags);

                    return new Block(id * groupSize + i * blockSize);
                }

            }

            throw new IllegalStateException("no free blocks " + id);
        }


        void deallocateBlock(final int blockId) throws IOException {
            final Flags.BlockGroupFlags currentBlockFlags = new Flags.BlockGroupFlags(cachedMetaBlockData[blockId]);
            currentBlockFlags.setTaken(false);
            freeBlocks++;
            updateBlockData(blockId, currentBlockFlags);
        }

        private void updateBlockData(final int blockId, final Flags.BlockGroupFlags currentBlockFlags) throws IOException {
            cachedMetaBlockData[blockId] = currentBlockFlags.value();

            dataFile.seek(id * groupSize + blockId);
            dataFile.write(currentBlockFlags.value());
        }

        private void initFreeBlocksCounter() throws IOException {
            if (freeBlocks == -1) {
                int free = 0;

                for (int i = 1; i < blockSize; i++) {
                    final Flags.BlockGroupFlags currentBlockFlags = new Flags.BlockGroupFlags(cachedMetaBlockData[i]);
                    if (!currentBlockFlags.isTaken()) {
                        free++;
                    }
                }

                freeBlocks = free;
            }
        }
    }

    public class Block {
        private final int address;

        private Block(final int address) {
            this.address = address;
        }

        public byte[] read() throws IOException {
            dataFile.seek(address * blockSize);
            final byte[] result = new byte[blockSize];
            dataFile.read(result);
            return result;
        }

        public void write(final byte[] bytes) throws IOException {
            checkArgument(bytes.length < blockSize, "unexpected block size " + bytes.length);
            dataFile.seek(address * blockSize);
            dataFile.write(bytes);
        }

        public int getAddress() {
            return address;
        }
    }

}
