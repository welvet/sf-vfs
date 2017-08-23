package sfvfs.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import static sfvfs.utils.Preconditions.*;
import static sfvfs.utils.Streams.iteratorToStream;

/**
 * @author alexey.kutuzov
 */
public class Directory {

    public static final String NAME_REGEXP = "[A-Za-z0-9\\-_.]+";
    public static final String NAME_REGEXP_AND_SLASH = "[A-Za-z0-9\\-_./]+";

    private final static int NULL_POINTER = 0;
    private final static int PTRLEN = 4;

    private final static int FLAGS_IDX = 0;
    private final static int FIRST_ENTITY_LIST_BLOCK_IDX = 1;

    private static final Logger log = LoggerFactory.getLogger(Directory.class);

    private final DataBlocks dataBlocks;
    private final DataBlocks.Block rootBlock;
    private final int lastEntityListInRootBlock;
    private final int maxNameLen;

    public Directory(final DataBlocks dataBlocks, final int address, final int maxNameLen) {
        checkNotNull(dataBlocks, "dataBlocks");
        checkArgument(address > 0, "address must be more than 0: %s", address);
        checkArgument(maxNameLen > 0, "max len must be more than 0 %s", maxNameLen);

        this.dataBlocks = dataBlocks;
        this.rootBlock = dataBlocks.getBlock(address);
        this.lastEntityListInRootBlock = rootBlock.size() / PTRLEN - 1;
        this.maxNameLen = maxNameLen;

        checkArgument(rootBlock.size() > 2 * maxNameLen, "block size must be at least 2 times bigger than max len");
    }

    public int getRootBlockAddress() {
        return rootBlock.getAddress();
    }

    public void create() throws IOException {
        rootBlock.clear();

        rootBlock.writeInt(FLAGS_IDX * PTRLEN, 0);

        final DataBlocks.Block firstPlainDirBlock = dataBlocks.allocateBlock();
        firstPlainDirBlock.clear();
        rootBlock.writeInt(FIRST_ENTITY_LIST_BLOCK_IDX * PTRLEN, firstPlainDirBlock.getAddress());

        log.debug("dir {} created", rootBlock.getAddress(), firstPlainDirBlock.getAddress());
    }

    public Iterator<DirectoryEntity> listEntities() throws IOException {
        return getAllEntityLists()
                .stream()
                .flatMap(entityList -> {
                    try {
                        return iteratorToStream(entityList.listEntities());
                    } catch (IOException io) {
                        throw new RuntimeException(io);
                    }
                })
                .iterator();
    }

    public int size() throws IOException {
        return getAllEntityLists()
                .stream()
                .mapToInt(entityList -> {
                    try {
                        return entityList.size();
                    } catch (IOException io) {
                        throw new RuntimeException(io);
                    }
                })
                .sum();
    }

    public void addEntity(final String name, final int address, final Flags.DirectoryListEntityFlags flags) throws IOException {
        checkNotNull(name, "name");
        checkNotNull(flags, "flags");
        checkArgument(address > 0, "address must be more than 0: %s", address);
        checkArgument(name.length() <= maxNameLen, "name len must be les than %s: %s", maxNameLen, name);
        checkArgument(name.matches(NAME_REGEXP), "name doesn't match %s %s", NAME_REGEXP, name);

        checkState(find(name) == null, "element %s already exists", name);

        getEntityList(name).addEntity(name, address, flags);
    }

    public DirectoryEntity find(final String name) throws IOException {
        checkNotNull(name, "name");

        final Iterator<DirectoryEntity> entityIterator = getEntityList(name).listEntities();
        while (entityIterator.hasNext()) {
            final DirectoryEntity entity = entityIterator.next();
            if (entity.getName().equals(name)) {
                return entity;
            }
        }

        return null;
    }

    public void removeEntity(final String name) throws IOException {
        checkNotNull(name, "name");

        getEntityList(name).remove(name);
    }

    public void delete() throws IOException {
        if (size() != 0) {
            throw new IOException("directory is not empty size=" + size());
        }

        for (final EntityList entityList : getAllEntityLists()) {
            entityList.delete();
        }

        dataBlocks.deallocateBlock(rootBlock.getAddress());
    }

    private List<EntityList> getAllEntityLists() throws IOException {
        final List<EntityList> result = new ArrayList<>();

        final ByteBuffer rootBlockData = ByteBuffer.wrap(rootBlock.read());
        for (int i = FIRST_ENTITY_LIST_BLOCK_IDX; i <= lastEntityListInRootBlock; i++) {
            final int entityListAddress = rootBlockData.getInt(i * PTRLEN);
            if (entityListAddress != NULL_POINTER) {
                result.add(new EntityList(entityListAddress));
            }
        }

        return result;
    }

    private EntityList getEntityList(final String name) throws IOException {
        if (getFlags().isIndexed()) {
            throw new UnsupportedOperationException();
        } else {
            return new EntityList(rootBlock.readInt(FIRST_ENTITY_LIST_BLOCK_IDX * PTRLEN));
        }
    }

    private Flags.DirectoryFlags getFlags() throws IOException {
        return new Flags.DirectoryFlags(rootBlock.readInt(FLAGS_IDX * PTRLEN));
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder();

        sb.append("Directory [address=").append(rootBlock.getAddress()).append("] ");
        try {
            final ByteBuffer rootBlockData = ByteBuffer.wrap(rootBlock.read());

            sb.append(" flags:").append(Integer.toBinaryString(rootBlockData.getInt(FLAGS_IDX * PTRLEN)));
            sb.append("\n");
            for (int i = FIRST_ENTITY_LIST_BLOCK_IDX; i <= lastEntityListInRootBlock; i++) {
                final int entityListAddress = rootBlockData.getInt(i * PTRLEN);

                if (entityListAddress != 0) {
                    sb.append(" i:").append(i).append(" ");
                    sb.append(new EntityList(entityListAddress).toString());
                    sb.append("\n");
                }
            }
        } catch (final IOException e) {
            sb.append("can't create toString: ").append(e.getMessage());
        }

        return sb.toString();
    }

    private class EntityList {

        private static final int SIZE_IDX = 0;
        private static final int NEXT_BLOCK_IDX = 1;
        private static final int FIRST_ENTITY_ADDRESS = 2 * PTRLEN;

        private final DataBlocks.Block entityListRootBlock;

        private EntityList(final int entityListBlockAddress) throws IOException {
            this.entityListRootBlock = dataBlocks.getBlock(entityListBlockAddress);
        }

        Iterator<DirectoryEntity> listEntities() throws IOException {

            return new Iterator<DirectoryEntity>() {
                private Node next;

                private DataBlocks.Block currentListBlock = entityListRootBlock;
                private ByteBuffer currentListBlockData;
                private int nextListBlockAddress;

                @Override
                public boolean hasNext() {
                    if (next != null) {
                        return true;
                    }

                    try {
                        while (currentListBlock != null) {
                            if (currentListBlockData == null) {
                                currentListBlockData = ByteBuffer.wrap(currentListBlock.read());
                                nextListBlockAddress = FIRST_ENTITY_ADDRESS;
                            }

                            next = readNodeIfExists(currentListBlock.getAddress(), currentListBlockData, nextListBlockAddress);
                            if (next != null) {
                                nextListBlockAddress += next.length() + 1;
                                break;
                            } else {
                                final int nextListBlockAddress = currentListBlockData.getInt(NEXT_BLOCK_IDX * PTRLEN);
                                if (nextListBlockAddress != 0) {
                                    currentListBlock = dataBlocks.getBlock(nextListBlockAddress);
                                } else {
                                    currentListBlock = null;
                                }
                                currentListBlockData = null;
                            }
                        }
                    } catch (final IOException io) {
                        throw new RuntimeException(io);
                    }

                    return next != null;
                }

                @Override
                public DirectoryEntity next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException();
                    }

                    try {
                        return next;
                    } finally {
                        next = null;
                    }
                }
            };
        }

        int size() throws IOException {
            return entityListRootBlock.readInt(SIZE_IDX * PTRLEN);
        }

        void addEntity(final String name, final int address, final Flags.DirectoryListEntityFlags flags) throws IOException {
            final Node newNode = new Node(address, name, flags, -1, -1);
            final int newNodeLen = newNode.length();

            DataBlocks.Block currentListBlock = entityListRootBlock;
            int currentAddressInListBlock = FIRST_ENTITY_ADDRESS;
            ByteBuffer currentListBlockData = ByteBuffer.wrap(currentListBlock.read());

            while (true) {
                while (currentAddressInListBlock + newNodeLen < currentListBlock.size()) {
                    final Node node = readNodeIfExists(currentListBlock.getAddress(), currentListBlockData, currentAddressInListBlock);
                    if (node != null) {
                        currentAddressInListBlock += node.length() + 1;
                    } else {
                        break;
                    }
                }

                if (currentAddressInListBlock + newNodeLen < currentListBlock.size()) {
                    break;
                }

                final int nextListBlockAddress = currentListBlockData.getInt(NEXT_BLOCK_IDX * PTRLEN);
                if (nextListBlockAddress != 0) {
                    currentListBlock = dataBlocks.getBlock(nextListBlockAddress);
                } else {
                    final DataBlocks.Block newBlock = dataBlocks.allocateBlock();
                    newBlock.clear();
                    currentListBlock.writeInt(NEXT_BLOCK_IDX * PTRLEN, newBlock.getAddress());
                    currentListBlock = newBlock;
                }

                currentListBlockData = ByteBuffer.wrap(currentListBlock.read());
                currentAddressInListBlock = FIRST_ENTITY_ADDRESS;
            }

            newNode.write(currentListBlockData, currentAddressInListBlock);
            currentListBlock.write(currentListBlockData.array());
            log.debug("+ entity {} to list={} part={} offset={}",
                    address, entityListRootBlock.getAddress(), currentListBlock.getAddress(), currentAddressInListBlock);

            entityListRootBlock.writeInt(SIZE_IDX * PTRLEN, size() + 1);
        }

        void remove(final String name) throws IOException {
            final Iterator<DirectoryEntity> entityIterator = listEntities();
            Node node = null;

            while (entityIterator.hasNext()) {
                final DirectoryEntity entity = entityIterator.next();
                if (entity.getName().equals(name)) {
                    node = (Node) entity;
                    break;
                }
            }

            if (node == null) {
                log.debug("not found for remove {} in {}", name, rootBlock.getAddress());
                return;
            }

            final DataBlocks.Block currentListBlock = dataBlocks.getBlock(node.entityNodeListAddress);
            final ByteBuffer currentListBlockData = ByteBuffer.wrap(currentListBlock.read());
            int addressInCurrentListBlock = FIRST_ENTITY_ADDRESS;

            final ByteBuffer listBlockNewData = ByteBuffer.wrap(new byte[currentListBlock.size()]);
            int addressInNewListBlock = FIRST_ENTITY_ADDRESS;

            while (true) {
                final Node anotherNode = readNodeIfExists(currentListBlock.getAddress(), currentListBlockData, addressInCurrentListBlock);
                if (anotherNode == null) {
                    break;
                }

                if (anotherNode.getAddress() != node.getAddress()) {
                    anotherNode.write(listBlockNewData, addressInNewListBlock);
                    addressInNewListBlock += anotherNode.length() + 1;
                }

                addressInCurrentListBlock += anotherNode.length() + 1;
            }

            if (currentListBlock.getAddress() == entityListRootBlock.getAddress() || addressInNewListBlock != FIRST_ENTITY_ADDRESS) {
                listBlockNewData.putInt(SIZE_IDX * PTRLEN, currentListBlockData.getInt(SIZE_IDX * PTRLEN));
                listBlockNewData.putInt(NEXT_BLOCK_IDX * PTRLEN, currentListBlockData.getInt(NEXT_BLOCK_IDX * PTRLEN));
                currentListBlock.write(listBlockNewData.array());

                log.debug("- entity {} from list={} part={}", node.getAddress(), entityListRootBlock.getAddress(), currentListBlock.getAddress());
            } else {
                final int currentBlockNextBlockAddress = currentListBlockData.getInt(NEXT_BLOCK_IDX * PTRLEN);

                DataBlocks.Block prevListBlock = entityListRootBlock;
                while (true) {
                    final int prevBlockNextAddress = prevListBlock.readInt(NEXT_BLOCK_IDX * PTRLEN);
                    if (prevBlockNextAddress == currentListBlock.getAddress()) {
                        prevListBlock.writeInt(NEXT_BLOCK_IDX * PTRLEN, currentBlockNextBlockAddress);
                        break;
                    } else {
                        prevListBlock = dataBlocks.getBlock(prevBlockNextAddress);
                    }
                }

                dataBlocks.deallocateBlock(currentListBlock.getAddress());
                log.debug("- entity {} with list part, list={} prev={} next={} gone=",
                        node.getAddress(), entityListRootBlock.getAddress(), prevListBlock.getAddress(),
                        currentBlockNextBlockAddress, currentListBlock.getAddress());
            }

            entityListRootBlock.writeInt(SIZE_IDX * PTRLEN, size() -1);
        }

        void delete() throws IOException {
            DataBlocks.Block currentListBlock = entityListRootBlock;

            while (true) {
                final int nextListBlockAddress = currentListBlock.readInt(NEXT_BLOCK_IDX * PTRLEN);

                dataBlocks.deallocateBlock(currentListBlock.getAddress());
                if (nextListBlockAddress != NULL_POINTER) {
                    currentListBlock = dataBlocks.getBlock(nextListBlockAddress);
                } else {
                    break;
                }
            }
        }

        private Node readNodeIfExists(final int entityNodeListAddress, final ByteBuffer byteBuffer, final int offset) {
            if (byteBuffer.array().length - offset < PTRLEN) {
                return null;
            }

            byteBuffer.position(offset);

            final int firstInodeBlockAddress = byteBuffer.getInt();
            if (firstInodeBlockAddress == NULL_POINTER) {
                return null;
            }

            final Flags.DirectoryListEntityFlags flags = new Flags.DirectoryListEntityFlags(byteBuffer.get());

            final int nameLength = byteBuffer.get();
            final byte[] nameBytes = new byte[nameLength];
            byteBuffer.get(nameBytes);

            return new Node(firstInodeBlockAddress, new String(nameBytes), flags, entityNodeListAddress, offset);
        }

        @Override
        public String toString() {
            final StringBuilder sb = new StringBuilder();

            sb.append("Entities [address=").append(entityListRootBlock.getAddress()).append("] \n");
            try {
                final Iterator<DirectoryEntity> entityIterator = listEntities();
                while (entityIterator.hasNext()) {
                    sb.append("  ").append(entityIterator.next()).append("\n");
                }
            } catch (final IOException io) {
                sb.append("can't list entities").append(io.toString());
            }

            return sb.toString();
        }

        private class Node implements DirectoryEntity {
            private final String name;
            private final int address;
            private final Flags.DirectoryListEntityFlags flags;
            private final int entityNodeListAddress;
            private final int offset;

            Node(final int inodeBlockAddress, final String name, final Flags.DirectoryListEntityFlags flags, final int entityNodeListAddress, final int offset) {
                this.name = name;
                this.address = inodeBlockAddress;
                this.flags = flags;
                this.entityNodeListAddress = entityNodeListAddress;
                this.offset = offset;
            }

            @Override
            public String getName() {
                return name;
            }

            @Override
            public int getAddress() {
                return address;
            }

            @Override
            public boolean isDirectory() {
                return flags.isDirectory();
            }

            @Override
            public int getParentDirectoryAddress() {
                return rootBlock.getAddress();
            }

            @Override
            public Flags.DirectoryListEntityFlags getFlags() {
                return new Flags.DirectoryListEntityFlags(flags.value());
            }

            int length() {
                return Flags.DirectoryListEntityFlags.LENGTH + PTRLEN + name.getBytes().length;
            }

            void write(final ByteBuffer byteBuffer, final int offset) {
                final int freeSpace = byteBuffer.array().length - offset;
                final int length = length();
                checkState(freeSpace >= length, "not enough space {}, {}", freeSpace, length);

                byteBuffer.position(offset);
                byteBuffer.putInt(address);
                byteBuffer.put(flags.value());

                final byte[] nameBytes = name.getBytes();
                byteBuffer.put((byte) nameBytes.length);
                byteBuffer.put(nameBytes);
            }

            @Override
            public String toString() {
                return String.format("Entity [address=%d] root=%d list=%s offset=%s len=%s name=%s",
                        address, rootBlock.getAddress(), entityNodeListAddress, offset, length(), name);
            }
        }
    }

}
