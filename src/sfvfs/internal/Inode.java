package sfvfs.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static sfvfs.utils.Preconditions.*;

/**
 * @author alexey.kutuzov
 */
public class Inode {

    private final static int NULL_POINTER = 0;
    private final static int PTRLEN = 4;

    private final static int FLAGS_IDX = 0;
    private final static int SIZE_IDX = 1;
    private final static int LAST_INODE_IDX = 2;
    private final static int FIRST_DATA_BLOCK_IDX = 3;

    private static final Logger log = LoggerFactory.getLogger(Inode.class);

    private final DataBlocks dataBlocks;
    private final int nextInodeBlockIndex;

    private final DataBlocks.Block rootInodeBlock;

    public Inode(final DataBlocks dataBlocks, final int address) throws IOException {
        checkNotNull(dataBlocks, "dataBlocks");
        checkArgument(address > 0, "address must be more than 0: %s", address);

        this.dataBlocks = dataBlocks;
        this.rootInodeBlock = dataBlocks.getBlock(address);

        this.nextInodeBlockIndex = (rootInodeBlock.size() / PTRLEN) - 1;
    }

    public int getSize() throws IOException {
        return this.rootInodeBlock.readInt(SIZE_IDX * PTRLEN);
    }

    public InputStream readStream() throws IOException {
        checkLockedAndLock();

        log.debug("IS open inode {}", Inode.this);
        return new InodeInputStream();
    }

    public OutputStream appendStream() throws IOException {
        checkLockedAndLock();

        DataBlocks.Block lastInodeBlock = rootInodeBlock;

        final int lastInodeAddress = rootInodeBlock.readInt(LAST_INODE_IDX * PTRLEN);
        if (lastInodeAddress != NULL_POINTER) {
            lastInodeBlock = dataBlocks.getBlock(lastInodeAddress);
        }

        final ByteBuffer lastInodeBlockData = ByteBuffer.wrap(lastInodeBlock.read());

        int lastDataBlockAddress = NULL_POINTER;
        int lastDataBlockIndexInInode = 0;
        for (int i = FIRST_DATA_BLOCK_IDX; i < nextInodeBlockIndex; i++) {
            final int nextAddress = lastInodeBlockData.getInt(i * PTRLEN);
            if (nextAddress != NULL_POINTER) {
                lastDataBlockAddress = nextAddress;
                lastDataBlockIndexInInode = i;
            } else {
                break;
            }
        }

        if (lastDataBlockAddress == NULL_POINTER) {
            lastDataBlockIndexInInode = FIRST_DATA_BLOCK_IDX;
            lastDataBlockAddress = dataBlocks.allocateBlock().getAddress();
        }

        final Flags.InodeFlags flags = getFlags();
        final int size = getSize();

        if (flags.isNeedEmptyBlock()) {
            lastDataBlockIndexInInode++;
            lastDataBlockAddress = dataBlocks.allocateBlock().getAddress();
        }

        return new InodeOutputStream(
                size,
                lastInodeBlock,
                lastInodeBlockData,
                lastDataBlockIndexInInode,
                dataBlocks.getBlock(lastDataBlockAddress),
                size % lastInodeBlock.size()
        );
    }

    public void clear() throws IOException {
        clear(false);
    }

    public void delete() throws IOException {
        clear(true);
    }

    int debugGetNextInodeAddress() throws IOException {
        return rootInodeBlock.readInt(nextInodeBlockIndex * PTRLEN);
    }

    private Flags.InodeFlags getFlags() throws IOException {
        return new Flags.InodeFlags(this.rootInodeBlock.readInt(FLAGS_IDX * PTRLEN));
    }

    private void clear(final boolean removeRootInode) throws IOException {
        DataBlocks.Block currentInodeBlock = rootInodeBlock;

        while (true) {
            final ByteBuffer currentInodeBlockData = ByteBuffer.wrap(currentInodeBlock.read());

            for (int i = FIRST_DATA_BLOCK_IDX; i < nextInodeBlockIndex; i++) {
                final int dataBlockAddress = currentInodeBlockData.getInt(i * PTRLEN);

                if (dataBlockAddress != NULL_POINTER) {
                    dataBlocks.deallocateBlock(dataBlockAddress);
                    log.debug("inode {} clear data block cleared {}", rootInodeBlock.getAddress(), dataBlockAddress);
                } else {
                    break;
                }
            }

            final int nextInodeAddress = currentInodeBlockData.getInt(nextInodeBlockIndex * PTRLEN);
            if (currentInodeBlock != rootInodeBlock) {
                dataBlocks.deallocateBlock(currentInodeBlock.getAddress());
                log.debug("inode {} clear inode block cleared {}", rootInodeBlock.getAddress(), nextInodeAddress);
            }

            if (nextInodeAddress != NULL_POINTER) {
                currentInodeBlock = dataBlocks.getBlock(nextInodeAddress);
                log.debug("inode {} clear inode block loaded {}", rootInodeBlock.getAddress(), nextInodeAddress);
            } else {
                break;
            }
        }

        if (removeRootInode) {
            dataBlocks.deallocateBlock(rootInodeBlock.getAddress());
        } else {
            rootInodeBlock.clear();
        }

        log.debug("inode {} cleared remove root:{}", rootInodeBlock.getAddress(), removeRootInode);
    }

    private void checkLockedAndLock() throws IOException {
        final Flags.InodeFlags flags = getFlags();
        checkState(!flags.isLocked(), "inode %s already locked, unlock first", rootInodeBlock.getAddress());
        flags.setLocked(true);
        rootInodeBlock.writeInt(FLAGS_IDX * PTRLEN, flags.value());
    }

    private void unlock() throws IOException {
        final Flags.InodeFlags flags = getFlags();
        checkState(flags.isLocked(), "inode %s was not locked", rootInodeBlock.getAddress());
        flags.setLocked(false);
        rootInodeBlock.writeInt(FLAGS_IDX * PTRLEN, flags.value());
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder();

        result.append("Inode [address = ").append(rootInodeBlock.getAddress()).append("] ");

        try {
            final ByteBuffer byteBuffer = ByteBuffer.wrap(rootInodeBlock.read());

            result.append(" flags:").append(new Flags.InodeFlags(byteBuffer.getInt(FLAGS_IDX * PTRLEN)));
            result.append(" size:").append(byteBuffer.getInt(SIZE_IDX * PTRLEN));
            result.append(" last:").append(byteBuffer.getInt(LAST_INODE_IDX * PTRLEN)).append(" ");

            for (int i = FIRST_DATA_BLOCK_IDX; i < nextInodeBlockIndex; i++) {
                result.append(byteBuffer.getInt(i * PTRLEN)).append(" ");
            }

            result.append(" next:").append(byteBuffer.getInt(nextInodeBlockIndex * PTRLEN)).append(" ");
        } catch (final IOException e) {
            result.append("сan't construct toString:").append(e.getMessage());
        }

        return result.toString();
    }

    public int getAddress() {
        return rootInodeBlock.getAddress();
    }

    private class InodeInputStream extends InputStream {

        private ByteBuffer inodeBlockData;
        private int dataBlockIndexInInode = FIRST_DATA_BLOCK_IDX;

        private byte[] dataBlockData;
        private int dataBlockIndexIndex;

        private int sizeLeft;

        private InodeInputStream() throws IOException {
            inodeBlockData = ByteBuffer.wrap(rootInodeBlock.read());
            sizeLeft = inodeBlockData.getInt(SIZE_IDX * PTRLEN);
        }

        @Override
        public int read() throws IOException {
            if (sizeLeft-- <= 0) {
                return -1;
            }

            if (dataBlockData == null) {
                int nextAddress = inodeBlockData.getInt(dataBlockIndexInInode * PTRLEN);

                if (dataBlockIndexInInode == nextInodeBlockIndex) {
                    inodeBlockData = ByteBuffer.wrap(dataBlocks.getBlock(nextAddress).read());
                    dataBlockIndexInInode = FIRST_DATA_BLOCK_IDX;
                    log.debug("IS inode {} inode block open {}", rootInodeBlock.getAddress(), nextAddress);
                    nextAddress = inodeBlockData.getInt(dataBlockIndexInInode * PTRLEN);
                }
                dataBlockIndexInInode++;

                dataBlockIndexIndex = 0;
                dataBlockData = dataBlocks.getBlock(nextAddress).read();
                log.debug("IS inode {} data block open {}", rootInodeBlock.getAddress(), nextAddress);
            }

            final byte result = dataBlockData[dataBlockIndexIndex++];
            if (dataBlockIndexIndex == dataBlockData.length) {
                dataBlockData = null;
            }

            return result & 0xFF;
        }

        @Override
        public void close() throws IOException {
            unlock();
        }
    }

    private class InodeOutputStream extends OutputStream {

        private DataBlocks.Block inodeBlock;
        private ByteBuffer inodeBlockData;
        private int dataBlockIndexInInode;

        private DataBlocks.Block dataBlock;
        private byte[] dataBlockData;
        private int dataBlockIndex;

        private int size;
        private int currentDataBlockSavedSize;

        private InodeOutputStream(final int size,
                                  final DataBlocks.Block inodeBlock,
                                  final ByteBuffer inodeBlockData,
                                  final int dataBlockIndexInInode,
                                  final DataBlocks.Block dataBlock,
                                  final int prevDataBlockIndex) throws IOException {
            log.debug("OS open inode {}, size {}, last inode {} {}", rootInodeBlock.getAddress(), size, inodeBlock.getAddress(), Inode.this);

            this.size = size;

            this.inodeBlock = inodeBlock;
            this.inodeBlockData = inodeBlockData;
            this.dataBlockIndexInInode = dataBlockIndexInInode;

            this.dataBlock = dataBlock;
            this.dataBlockData = dataBlock.read();
            this.dataBlockIndex = prevDataBlockIndex;
            this.currentDataBlockSavedSize = prevDataBlockIndex;
        }

        @Override
        public void write(final int b) throws IOException {
            checkState(inodeBlock != null, "stream closed");

            if (dataBlockIndex == inodeBlock.size()) {
                createNextInodeIfNecessary();

                dataBlock.write(dataBlockData);
                size += dataBlockIndex - currentDataBlockSavedSize;
                inodeBlockData.putInt(dataBlockIndexInInode++ * PTRLEN, dataBlock.getAddress());

                dataBlock = dataBlocks.allocateBlock();
                dataBlockData = new byte[dataBlock.size()];
                currentDataBlockSavedSize = 0;
                dataBlockIndex = 0;

                log.debug("OS inode {} new data block {}", rootInodeBlock.getAddress(), dataBlock.getAddress());
            }

            dataBlockData[dataBlockIndex++] = (byte) b;
        }

        @Override
        public void flush() throws IOException {
            checkState(inodeBlock != null, "stream closed");

            createNextInodeIfNecessary();
            inodeBlockData.putInt(dataBlockIndexInInode * PTRLEN, dataBlock.getAddress());
            inodeBlock.write(inodeBlockData.array());
            dataBlock.write(dataBlockData);

            size += dataBlockIndex - currentDataBlockSavedSize;
            currentDataBlockSavedSize = dataBlockIndex;

            rootInodeBlock.writeInt(SIZE_IDX * PTRLEN, size);
            if (inodeBlock == rootInodeBlock) {
                inodeBlockData.putInt(SIZE_IDX * PTRLEN, size);
            }

            log.debug("OS flush inode {}, size {}, last inode {} {}", rootInodeBlock.getAddress(), size, inodeBlock.getAddress(), Inode.this);
        }

        @Override
        public void close() throws IOException {
            flush();

            final Flags.InodeFlags flags = getFlags();
            if (size % dataBlock.size() == 0 && size > 0) {
                flags.setNeedEmptyBlock(true);
            } else {
                flags.setNeedEmptyBlock(false);
            }
            rootInodeBlock.writeInt(FLAGS_IDX * PTRLEN, flags.value());

            if (inodeBlock != rootInodeBlock) {
                rootInodeBlock.writeInt(LAST_INODE_IDX * PTRLEN, inodeBlock.getAddress());
            }

            log.debug("OS closed inode {}, size {}, last inode {} {}", rootInodeBlock.getAddress(), size, inodeBlock.getAddress(), Inode.this);

            inodeBlock = null;
            unlock();
        }

        private void createNextInodeIfNecessary() throws IOException {
            if (dataBlockIndexInInode == nextInodeBlockIndex) {
                final DataBlocks.Block newInodeBlock = dataBlocks.allocateBlock();
                newInodeBlock.clear();

                inodeBlockData.putInt(nextInodeBlockIndex * PTRLEN, newInodeBlock.getAddress());
                inodeBlock.write(inodeBlockData.array());

                inodeBlock = newInodeBlock;
                inodeBlockData = ByteBuffer.wrap(newInodeBlock.read());

                log.debug("OS inode {} created new inode {}", rootInodeBlock.getAddress(), newInodeBlock.getAddress());

                dataBlockIndexInInode = FIRST_DATA_BLOCK_IDX;
            }
        }
    }

}
