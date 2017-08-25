package sfvfs.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;

import static sfvfs.utils.Preconditions.*;

/**
 * @author alexey.kutuzov
 */
public class DataBlocks implements AutoCloseable {

    private static final int MAX_BLOCKS_MAX_VALUE = 4 * 1024 * 1024;
    private static final int FREE_BLOCKS_NOT_INITIALIZED = -1;
    private static final int FIRST_AVAILABLE_BLOCK = 1;

    private static final int NULL_POINTER = 0;
    private static final int PTRLEN = 4;

    private static final Logger log = LoggerFactory.getLogger(DataBlocks.class);

    private final int blockSize;
    private final int groupSize;
    private final int blocksInGroup;
    private final int blockGroupsWithFreeBlocksCacheSize;
    private final int dataFileOffset;
    private final int maxBlocks;
    private final int freeLogicalAddressCacheSize;
    private final RandomAccessFile dataFile;

    private final Map<Integer, BlockGroup> blockGroupsWithFreeBlocks = new HashMap<>();
    private final Queue<Integer> freeLogicalAddressCache;
    private final int[] logicalToPhysicalAddressCache;
    private int addressMappingVersion;
    private int allocatedGroups;

    private int circularBlockGroupAllocationLastIndex = 0;
    private int circularLogicalAddressAllocationLastIndex = 0;

    public DataBlocks(
            final File file,
            final int blockSize,
            final int blockGroupsWithFreeBlocksCacheSize,
            final String mode,
            final int maxBlocks,
            final int freeLogicalAddressCacheSize
    ) throws IOException {
        checkNotNull(file, "file");
        checkArgument(blockSize > 0, "block size must be more than zero");
        checkArgument(((blockSize & -blockSize) == blockSize), "block size must be power of 2");
        checkArgument(freeLogicalAddressCacheSize > 0, "free logical address cache must be more than 0");
        checkArgument(maxBlocks <= MAX_BLOCKS_MAX_VALUE, "max blocks can't be more than %s", MAX_BLOCKS_MAX_VALUE);
        checkArgument(maxBlocks > 0, "max blocks size must be more than 0");
        checkArgument(maxBlocks % blockSize == 0, "max blocks must be divisible by block size");

        this.dataFile = new RandomAccessFile(file, mode);

        this.blockSize = blockSize;
        this.groupSize = blockSize * blockSize;
        this.blocksInGroup = blockSize;
        this.blockGroupsWithFreeBlocksCacheSize = blockGroupsWithFreeBlocksCacheSize;
        this.maxBlocks = maxBlocks;
        this.freeLogicalAddressCacheSize = freeLogicalAddressCacheSize;

        this.logicalToPhysicalAddressCache = new int[maxBlocks];
        this.freeLogicalAddressCache = new ArrayDeque<>(freeLogicalAddressCacheSize);

        log.info("blocks created on {} blocks size {} mode {}", file.getAbsoluteFile(), blockSize, mode);

        final int logicalAddressMappingRegionLen = maxBlocks * PTRLEN;
        dataFileOffset = logicalAddressMappingRegionLen;
        if (dataFile.length() == 0) {
            final byte[] empty = new byte[blockSize];
            for (int i = 0; i <logicalAddressMappingRegionLen; i += blockSize) {
                dataFile.write(empty);
            }
            checkState(dataFile.length() == dataFileOffset, "wrong file size");
            log.debug("logical address mapping region allocated 0 - {}", logicalAddressMappingRegionLen);
        }

        final long blocksDataLen = this.dataFile.length() - dataFileOffset;
        checkState(blocksDataLen % groupSize == 0, "block size doesn't match file size");
        this.allocatedGroups = (int) (blocksDataLen / groupSize);
    }

    public Block allocateBlock() throws IOException {
        if (blockGroupsWithFreeBlocks.isEmpty()) {
            int nextCircularBlockGroupAllocationLasIndex = 0;

            for (int i = 0; i < allocatedGroups; i++) {
                final int groupId = (i + circularBlockGroupAllocationLastIndex) % allocatedGroups;
                nextCircularBlockGroupAllocationLasIndex = groupId;

                final BlockGroup blockGroup = getBlockGroup(groupId);

                if (blockGroup.hasFreeBlocks()) {
                    blockGroupsWithFreeBlocks.put(groupId, blockGroup);
                    if (blockGroupsWithFreeBlocks.size() >= blockGroupsWithFreeBlocksCacheSize) {
                        break;
                    }
                }
            }
            circularBlockGroupAllocationLastIndex = nextCircularBlockGroupAllocationLasIndex;

            while (blockGroupsWithFreeBlocks.size() < blockGroupsWithFreeBlocksCacheSize) {
                circularBlockGroupAllocationLastIndex++;
                final BlockGroup newGroup = new BlockGroup(allocatedGroups++);
                blockGroupsWithFreeBlocks.put(newGroup.id, newGroup);
                log.debug("group allocated {} {} - {}", newGroup.id, newGroup.id * groupSize, (newGroup.id + 1) * groupSize);
            }
        }

        final BlockGroup blockGroup = blockGroupsWithFreeBlocks.values().iterator().next();
        try {
            return blockGroup.allocateBlock();
        } finally {
            if (!blockGroup.hasFreeBlocks()) {
                log.debug("group {} full, removing from pool", blockGroup.id);
                blockGroupsWithFreeBlocks.remove(blockGroup.id);
            }
        }
    }

    void deallocateBlock(final int logicalBlockAddress) throws IOException {
        final int physicalBlockAddress = resolvePhysicalAddress(logicalBlockAddress);
        final BlockGroup blockGroup = getBlockGroup(physicalBlockAddress / blocksInGroup);
        blockGroup.deallocateBlock(physicalBlockAddress % blocksInGroup);
        releaseLogicalAddress(logicalBlockAddress);

        if (blockGroupsWithFreeBlocks.size() < blockGroupsWithFreeBlocksCacheSize) {
            if (!blockGroupsWithFreeBlocks.containsKey(blockGroup.id)) {
                blockGroupsWithFreeBlocks.put(blockGroup.id, blockGroup);
                log.debug("group {} has {} empty slots, adding to pool", blockGroup.id, blockGroup.getFreeBlocks());
            }
        }
    }

    Block getBlock(final int logicalAddress) throws IOException {
        checkArgument(logicalAddress > 0, "block address must be more than 0");

        final int physicalAddress = resolvePhysicalAddress(logicalAddress);
        return new Block(logicalAddress, physicalAddress);
    }

    public void compact() throws IOException {
        if (log.isInfoEnabled()) {
            log.info("compact start blocks={} free={} fsize={}", getTotalBlocks(), getFreeBlocks(), dataFile.length());
        }

        int startGroup = -1;
        int endGroup = allocatedGroups;

        BlockGroup sourceBlockGroup = null;
        BlockGroup targetBlockGroup = null;

        final int[] physicalToLogicalAddressCache = obtainAllPhysicalToLogicalAddressMap();

        while (startGroup < endGroup) {
            while (targetBlockGroup == null && startGroup < endGroup) {
                targetBlockGroup = new BlockGroup(++startGroup);
                if (!targetBlockGroup.hasFreeBlocks()) {
                    targetBlockGroup = null;
                }
            }

            while (sourceBlockGroup == null && endGroup > startGroup) {
                sourceBlockGroup = new BlockGroup(--endGroup);
                if (sourceBlockGroup.isEmpty()) {
                    sourceBlockGroup.deallocateGroup();
                    sourceBlockGroup = null;
                }
            }

            if (startGroup < endGroup && targetBlockGroup != null && sourceBlockGroup != null) {
                while (targetBlockGroup.hasFreeBlocks() && !sourceBlockGroup.isEmpty()) {
                    final int sourceBlockIdInGroup = sourceBlockGroup.nextAllocatedBlockIdFromCache();
                    final int sourceBlockPhysicalAddress = sourceBlockGroup.id * blocksInGroup + sourceBlockIdInGroup;
                    final int sourceBlockLogicalAddress = physicalToLogicalAddressCache[sourceBlockPhysicalAddress];
                    checkState(sourceBlockLogicalAddress != NULL_POINTER, "logical address %s no taken but assigned to %s",
                            sourceBlockLogicalAddress, sourceBlockPhysicalAddress);

                    final Block sourceBlock = new Block(sourceBlockLogicalAddress, sourceBlockPhysicalAddress);
                    final Block targetBlock = targetBlockGroup.allocateBlock();

                    targetBlock.write(sourceBlock.read());

                    reassignPhysicalAddress(sourceBlockLogicalAddress, sourceBlockPhysicalAddress, targetBlock.physicalAddress);

                    sourceBlockGroup.deallocateBlock(sourceBlockIdInGroup);
                }

                if (!targetBlockGroup.hasFreeBlocks()) {
                    targetBlockGroup = null;
                }

                if (sourceBlockGroup.isEmpty()) {
                    sourceBlockGroup.deallocateGroup();
                    sourceBlockGroup = null;
                }
            }
        }

        freeLogicalAddressCache.clear();
        blockGroupsWithFreeBlocks.clear();
        addressMappingVersion++;

        if (log.isInfoEnabled()) {
            log.info("compact competed blocks={} free={} fsize={}", getTotalBlocks(), getFreeBlocks(), dataFile.length());
        }
    }

    public int getTotalBlocks() {
        return allocatedGroups * blocksInGroup;
    }

    public int getFreeBlocks() throws IOException {
        int result = 0;
        for (int i = 0; i < allocatedGroups; i++) {
            result += getBlockGroup(i).getFreeBlocks();
        }

        return result;
    }

    public int getBlockSize() {
        return blockSize;
    }

    void debugPrintBlockUsage() throws IOException {
        for (int i = 0; i < allocatedGroups; i++) {
            log.info("group {} free {}", i, new BlockGroup(i).getFreeBlocks());
        }
    }

    @Override
    public void close() throws Exception {
        dataFile.close();
    }

    private int acquireLogicalAddress(final int physicalAddress) throws IOException {
        checkArgument(physicalAddress > 0, "physicalAddress must be more than 0");

        if (freeLogicalAddressCache.isEmpty()) {
            int nextCircularLogicalAddressAllocationLastIndex = 0;

            for (int i = 0; i < maxBlocks; i++) {
                final int logicalAddress = (i + circularLogicalAddressAllocationLastIndex) % maxBlocks;
                nextCircularLogicalAddressAllocationLastIndex = logicalAddress;

                if (logicalAddress < FIRST_AVAILABLE_BLOCK) {
                    continue;
                }

                dataFile.seek(logicalAddress * PTRLEN);
                final int associatedPhysicalAddress = dataFile.readInt();

                if (associatedPhysicalAddress == NULL_POINTER) {
                    freeLogicalAddressCache.add(logicalAddress);
                    if (freeLogicalAddressCache.size() >= freeLogicalAddressCacheSize) {
                        break;
                    }
                }
            }

            circularLogicalAddressAllocationLastIndex = nextCircularLogicalAddressAllocationLastIndex;
        }

        checkState(!freeLogicalAddressCache.isEmpty(), "no empty blocks left");

        final Integer logicalAddress = freeLogicalAddressCache.poll();
        dataFile.seek(logicalAddress * PTRLEN);
        dataFile.writeInt(physicalAddress);

        logicalToPhysicalAddressCache[logicalAddress] = physicalAddress;

        log.debug("address mapping created {} -> {}", logicalAddress, physicalAddress);

        return logicalAddress;
    }

    private int resolvePhysicalAddress(final int logicalAddress) throws IOException {
        checkArgument(logicalAddress > 0, "logicalAddress must be more than 0");

        final int cachedPhysicalAddress = logicalToPhysicalAddressCache[logicalAddress];

        if (cachedPhysicalAddress != NULL_POINTER) {
            return cachedPhysicalAddress;
        }

        dataFile.seek(logicalAddress * PTRLEN);
        final int associatedPhysicalAddress = dataFile.readInt();

        checkState(associatedPhysicalAddress != NULL_POINTER, "address %s not allocated", logicalAddress);

        logicalToPhysicalAddressCache[logicalAddress] = associatedPhysicalAddress;

        return associatedPhysicalAddress;
    }

    private void reassignPhysicalAddress(final int logicalAddress, final int oldPhysicalAddress, final int newPhysicalAddress) throws IOException {
        checkArgument(logicalAddress > 0, "logicalAddress must be more than 0");
        checkArgument(oldPhysicalAddress > 0, "oldPhysicalAddress must be more than 0");
        checkArgument(newPhysicalAddress > 0, "newPhysicalAddress must be more than 0");
        checkArgument(oldPhysicalAddress != newPhysicalAddress, "oldPhysicalAddress and newPhysicalAddress must be different");

        dataFile.seek(logicalAddress * PTRLEN);
        final int associatedPhysicalAddress = dataFile.readInt();

        checkState(associatedPhysicalAddress == oldPhysicalAddress, "wrong old physical address %s %s",
                associatedPhysicalAddress, oldPhysicalAddress);

        dataFile.seek(logicalAddress * PTRLEN);
        dataFile.writeInt(newPhysicalAddress);

        logicalToPhysicalAddressCache[logicalAddress] = newPhysicalAddress;

        log.debug("address mapping updated {} -> {} (was={})", logicalAddress, newPhysicalAddress, oldPhysicalAddress);
    }

    private int[] obtainAllPhysicalToLogicalAddressMap() throws IOException {
        final int[] physicalToLogicalAddress = new int[maxBlocks];

        for (int logicalAddress = FIRST_AVAILABLE_BLOCK; logicalAddress < maxBlocks; logicalAddress++) {
            dataFile.seek(logicalAddress * PTRLEN);

            final int physicalAddress = dataFile.readInt();
            if (physicalAddress != NULL_POINTER) {
                physicalToLogicalAddress[physicalAddress] = logicalAddress;
            }
        }

        return physicalToLogicalAddress;
    }

    private void releaseLogicalAddress(final int logicalAddress) throws IOException {
        checkArgument(logicalAddress > 0, "logicalAddress must be more than 0");

        dataFile.seek(logicalAddress * PTRLEN);
        dataFile.writeInt(NULL_POINTER);

        logicalToPhysicalAddressCache[logicalAddress] = NULL_POINTER;

        log.debug("address mapping released {}", logicalAddress);

        if (freeLogicalAddressCache.size() >= freeLogicalAddressCacheSize) {
            freeLogicalAddressCache.add(logicalAddress);
        }
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
        private final byte[] cachedMetaBlockData;

        private int freeBlocks = FREE_BLOCKS_NOT_INITIALIZED;
        private int circularFreeBlockLastIndex = 0;
        private Deque<Integer> nextAllocatedBlockCache = new ArrayDeque<>();

        BlockGroup(final int id) throws IOException {
            this.id = id;

            if (dataFile.length() <= dataFileOffset + id * groupSize) {
                dataFile.seek(dataFileOffset + id * groupSize);
                final byte[] emptyBytes = new byte[blockSize];
                for (int i = 0; i < blocksInGroup; i++) {
                    dataFile.write(emptyBytes);
                }

                cachedMetaBlockData = emptyBytes;
                freeBlocks = blocksInGroup - 1;

                log.debug("group allocated {}", id);
            } else {
                dataFile.seek(dataFileOffset + id * groupSize);
                final byte[] blockData = new byte[blockSize];
                dataFile.read(blockData);

                cachedMetaBlockData = blockData;
            }
        }

        boolean hasFreeBlocks() {
            initFreeBlocksCounter();

            return freeBlocks > 0;
        }

        boolean isEmpty() {
            initFreeBlocksCounter();

            return freeBlocks == blocksInGroup - FIRST_AVAILABLE_BLOCK;
        }

        int getFreeBlocks() {
            initFreeBlocksCounter();

            return freeBlocks;
        }

        Block allocateBlock() throws IOException {
            initFreeBlocksCounter();
            checkState(freeBlocks > 0, "no free blocks for %s", id);

            int nextCircularFreeBlockLastIndex;
            for (int i = 0; i < blocksInGroup; i++) {
                final int blockId = (i + circularFreeBlockLastIndex) % blocksInGroup;
                nextCircularFreeBlockLastIndex = blockId;
                if (blockId < FIRST_AVAILABLE_BLOCK) {
                    continue;
                }

                final Flags.BlockGroupFlags currentBlockFlags = new Flags.BlockGroupFlags(cachedMetaBlockData[blockId]);

                if (!currentBlockFlags.isTaken()) {
                    currentBlockFlags.setTaken(true);

                    freeBlocks--;
                    updateBlockData(blockId, currentBlockFlags);

                    log.debug("<- block={} group={}", blockId, id);

                    final int physicalAddress = id * blocksInGroup + blockId;
                    final int logicalAddress = acquireLogicalAddress(physicalAddress);

                    circularFreeBlockLastIndex = nextCircularFreeBlockLastIndex;
                    nextAllocatedBlockCache = null;

                    return new Block(logicalAddress, physicalAddress);
                }

            }

            throw new IllegalStateException("no free blocks " + id);
        }

        void deallocateBlock(final int blockId) throws IOException {
            initFreeBlocksCounter();

            final Flags.BlockGroupFlags currentBlockFlags = new Flags.BlockGroupFlags(cachedMetaBlockData[blockId]);
            currentBlockFlags.setTaken(false);
            freeBlocks++;
            updateBlockData(blockId, currentBlockFlags);

            log.debug("-> block {} group {}", blockId, id);
        }

        int nextAllocatedBlockIdFromCache() {
            if (nextAllocatedBlockCache == null || nextAllocatedBlockCache.isEmpty()) {
                nextAllocatedBlockCache = new ArrayDeque<>();

                for (int blockId = FIRST_AVAILABLE_BLOCK; blockId < blocksInGroup; blockId++) {
                    final Flags.BlockGroupFlags currentBlockFlags = new Flags.BlockGroupFlags(cachedMetaBlockData[blockId]);
                    if (currentBlockFlags.isTaken()) {
                        nextAllocatedBlockCache.add(blockId);
                    }
                }
            }

            return nextAllocatedBlockCache.pop();
        }

        void deallocateGroup() throws IOException {
            checkState(allocatedGroups > 0, "group must be more than 0");
            checkState(allocatedGroups - 1 == id, "can't release non last group %s %s", allocatedGroups, id);

            final int oldLength = (int) dataFile.length();
            final int newLength = dataFileOffset + (id) * groupSize;

            checkState(newLength == oldLength - groupSize, "unexpected new length %s %s", newLength, oldLength);
            dataFile.setLength(newLength);

            allocatedGroups--;

            log.debug("group release {} (len old={} new={})", id, oldLength, newLength);
        }

        private void updateBlockData(final int blockId, final Flags.BlockGroupFlags currentBlockFlags) throws IOException {
            cachedMetaBlockData[blockId] = currentBlockFlags.value();

            dataFile.seek(dataFileOffset + id * groupSize + blockId);
            dataFile.write(currentBlockFlags.value());
        }

        private void initFreeBlocksCounter() {
            if (freeBlocks == FREE_BLOCKS_NOT_INITIALIZED) {
                int free = 0;

                for (int i = FIRST_AVAILABLE_BLOCK; i < blocksInGroup; i++) {
                    final Flags.BlockGroupFlags currentBlockFlags = new Flags.BlockGroupFlags(cachedMetaBlockData[i]);
                    if (!currentBlockFlags.isTaken()) {
                        free++;
                    }
                }

                freeBlocks = free;
            }
        }

        @Override
        public String toString() {
            return "BlockGroup id=" + id + " " + (id * groupSize) + " " + ((id + 1) * groupSize - 1);
        }
    }

    public class Block {
        private final int addressMappingVersionWhenCreated;
        private final int logicalAddress;
        private final int physicalAddress;

        private Block(final int logicalAddress, final int physicalAddress) {
            this.addressMappingVersionWhenCreated = addressMappingVersion;
            this.logicalAddress = logicalAddress;
            this.physicalAddress = physicalAddress;
        }

        byte[] read() throws IOException {
            checkAddressMappingVersion();

            dataFile.seek(dataFileOffset + physicalAddress * blockSize);
            final byte[] result = new byte[blockSize];
            dataFile.read(result);
            return result;
        }

        int readInt(final int position) throws IOException {
            checkArgument(position >= 0 && position < blockSize, "unexpected position %s", position);
            checkAddressMappingVersion();

            dataFile.seek(dataFileOffset + physicalAddress * blockSize + position);
            return dataFile.readInt();
        }

        void write(final byte[] bytes) throws IOException {
            checkArgument(bytes.length <= blockSize, "unexpected block size %s", bytes.length);
            checkAddressMappingVersion();

            dataFile.seek(dataFileOffset + physicalAddress * blockSize);
            dataFile.write(bytes);
        }

        void writeInt(final int position, final int value) throws IOException {
            checkArgument(position >= 0 && position < blockSize, "unexpected position %s", position);
            checkAddressMappingVersion();

            dataFile.seek(dataFileOffset + physicalAddress * blockSize + position);
            dataFile.writeInt(value);
        }

        public void clear() throws IOException {
            checkAddressMappingVersion();

            write(new byte[blockSize]);
        }

        public int getAddress() {
            return logicalAddress;
        }

        int size() {
            return blockSize;
        }

        private void checkAddressMappingVersion() throws IOException {
            if (addressMappingVersion != addressMappingVersionWhenCreated) {
                throw new IOException("block outdated");
            }
        }
    }

}
