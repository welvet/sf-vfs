package sfvfs.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static sfvfs.utils.Preconditions.checkArgument;
import static sfvfs.utils.Preconditions.checkNotNull;
import static sfvfs.utils.Preconditions.checkState;

/**
 * @author alexey.kutuzov
 */
public class Inode {

    private final DataBlocks dataBlocks;
    private final int nextInodeIndex;

    private final DataBlocks.Block rootBlock;

    public Inode(final DataBlocks dataBlocks, final int address) throws IOException {
        checkNotNull(dataBlocks, "dataBlocks");
        checkArgument(address > 0, "address must be more than 0: " + address);

        this.dataBlocks = dataBlocks;
        this.rootBlock = dataBlocks.getBlock(address);

        this.nextInodeIndex = (rootBlock.size() / 4) - 1;
    }

    public Flags.InodeFlags getFlags() throws IOException {
        return new Flags.InodeFlags(this.rootBlock.readInt(0));
    }

    public int getSize() throws IOException {
        return this.rootBlock.readInt(4);
    }

    public InputStream readStream() throws IOException {
        return new InodeInputStream();
    }

    public OutputStream appendStream() throws IOException {
        DataBlocks.Block currentInodeBlock = rootBlock;

        while (true) {
            final int nextAddress = currentInodeBlock.readInt(4 * nextInodeIndex);
            if (nextAddress != 0) {
                currentInodeBlock = dataBlocks.getBlock(nextAddress);
            } else {
                break;
            }
        }

        final ByteBuffer currentInodeBlockData = ByteBuffer.wrap(currentInodeBlock.read());

        int lastDataBlockAddress = 0;
        int dataBlockIndexInInode = 0;
        for (int i = 2; i < nextInodeIndex; i++) {
            final int nextAddress = currentInodeBlockData.getInt(i * 4);
            if (nextAddress != 0) {
                lastDataBlockAddress = nextAddress;
                dataBlockIndexInInode = i;
            } else {
                break;
            }
        }

        if (lastDataBlockAddress == 0) {
            dataBlockIndexInInode = 2;
            lastDataBlockAddress = dataBlocks.allocateBlock().getAddress();
        }

        final Flags.InodeFlags flags = getFlags();
        final int size = getSize();

        if (flags.needEmptyBlock()) {
            dataBlockIndexInInode++;
            lastDataBlockAddress = dataBlocks.allocateBlock().getAddress();
        }

        return new InodeOutputStream(
                size,
                currentInodeBlock,
                currentInodeBlockData,
                dataBlockIndexInInode,
                dataBlocks.getBlock(lastDataBlockAddress),
                size % currentInodeBlock.size()
        );
    }

    public void clear() throws IOException {
        clear(false);
    }

    public void delete() throws IOException {
        clear(true);
    }

    int debugGetNextInode() throws IOException {
        return rootBlock.readInt(nextInodeIndex * 4);
    }

    private void clear(final boolean removeRootInode) throws IOException {
        DataBlocks.Block currentInodeBlock = rootBlock;

        while (true) {
            final ByteBuffer byteBuffer = ByteBuffer.wrap(currentInodeBlock.read());

            for (int i = 2; i < nextInodeIndex; i++) {
                final int dataBlockIndex = byteBuffer.getInt(i * 4);

                if (dataBlockIndex != 0) {
                    dataBlocks.deallocateBlock(dataBlockIndex);
                } else {
                    break;
                }
            }

            final int nextInodeAddress = byteBuffer.getInt(nextInodeIndex * 4);
            if (currentInodeBlock != rootBlock) {
                dataBlocks.deallocateBlock(currentInodeBlock.getAddress());
            }

            if (nextInodeAddress != 0) {
                currentInodeBlock = dataBlocks.getBlock(nextInodeAddress);
            } else {
                break;
            }
        }

        if (removeRootInode) {
            dataBlocks.deallocateBlock(rootBlock.getAddress());
        } else {
            rootBlock.clear();
        }
    }

    private class InodeInputStream extends InputStream {

        private ByteBuffer currentInodeBlockData;
        private int currentDataIndexBlockInInode = 1;

        private byte[] currentDataBlockData;
        private int currentDataIndex;

        private int sizeLeft;

        private InodeInputStream() throws IOException {
            currentInodeBlockData = ByteBuffer.wrap(rootBlock.read());
            sizeLeft = currentInodeBlockData.getInt(4);
        }

        @Override
        public int read() throws IOException {
            if (sizeLeft-- <= 0) {
                return -1;
            }

            if (currentDataBlockData == null) {
                int nextAddress = currentInodeBlockData.getInt(++currentDataIndexBlockInInode * 4);

                if (currentDataIndexBlockInInode == nextInodeIndex) {
                    currentInodeBlockData = ByteBuffer.wrap(dataBlocks.getBlock(nextAddress).read());
                    currentDataIndexBlockInInode = 2;
                    nextAddress = currentInodeBlockData.getInt(currentDataIndexBlockInInode * 4);
                }

                currentDataIndex = 0;
                currentDataBlockData = dataBlocks.getBlock(nextAddress).read();
            }

            final byte result = currentDataBlockData[currentDataIndex++];
            if (currentDataIndex == currentDataBlockData.length) {
                currentDataBlockData = null;
            }

            return result;
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
                inodeBlockData.putInt(dataBlockIndexInInode++ * 4, dataBlock.getAddress());

                dataBlock = dataBlocks.allocateBlock();
                dataBlockData = new byte[dataBlock.size()];
                currentDataBlockSavedSize = 0;
                dataBlockIndex = 0;
            }

            dataBlockData[dataBlockIndex++] = (byte) b;
        }

        @Override
        public void flush() throws IOException {
            checkState(inodeBlock != null, "stream closed");

            createNextInodeIfNecessary();
            inodeBlockData.putInt(dataBlockIndexInInode * 4, dataBlock.getAddress());
            inodeBlock.write(inodeBlockData.array());
            dataBlock.write(dataBlockData);

            size += dataBlockIndex - currentDataBlockSavedSize;
            currentDataBlockSavedSize = dataBlockIndex;

            rootBlock.writeInt(4, size);
            if (inodeBlock == rootBlock) {
                inodeBlockData.putInt(4, size);
            }
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
            rootBlock.writeInt(0, flags.value());

            inodeBlock = null;
        }

        private void createNextInodeIfNecessary() throws IOException {
            if (dataBlockIndexInInode == nextInodeIndex) {
                final DataBlocks.Block newInodeBlock = dataBlocks.allocateBlock();
                newInodeBlock.clear();

                inodeBlockData.putInt(nextInodeIndex * 4, newInodeBlock.getAddress());
                inodeBlock.write(inodeBlockData.array());

                inodeBlock = newInodeBlock;
                inodeBlockData = ByteBuffer.wrap(newInodeBlock.read());

                dataBlockIndexInInode = 2;
            }
        }
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder();

        result.append("Inode [address = ").append(rootBlock.getAddress()).append("] ");

        try {
            final ByteBuffer byteBuffer = ByteBuffer.wrap(rootBlock.read());

            result.append(new Flags.InodeFlags(byteBuffer.getInt(0)));

            result.append(" size:").append(byteBuffer.getInt(4)).append(" ");

            for (int i = 2; i < nextInodeIndex; i++) {
                result.append(byteBuffer.getInt(i * 4)).append(" ");
            }

            result.append(" next:").append(byteBuffer.getInt(nextInodeIndex * 4)).append(" ");
        } catch (final IOException e) {
            result.append("сan't construct toString:").append(e.getMessage());
        }

        return result.toString();
    }
}
