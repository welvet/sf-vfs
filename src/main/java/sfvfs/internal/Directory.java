package sfvfs.internal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static sfvfs.utils.Preconditions.*;
import static sfvfs.utils.Streams.iteratorToStream;

/**
 * @author alexey.kutuzov
 */
@SuppressWarnings("WeakerAccess")
public class Directory {

    public static final String NAME_REGEXP = "[A-Za-z0-9${}\\-_.]+";
    public static final String NAME_REGEXP_AND_SLASH = "[A-Za-z0-9${}\\-_./]+";

    private final static int NULL_POINTER = 0;
    private final static int PTRLEN = 4;

    private final static int FLAGS_IDX = 0;
    private final static int FIRST_ENTITY_LIST_BLOCK_IDX = 1;

    private static final Logger log = LoggerFactory.getLogger(Directory.class);

    private final DataBlocks dataBlocks;
    private final DataBlocks.Block rootBlock;
    private final int lastEntityListInRootBlock;
    private final int maxNameLen;
    private final int directoryMinSizeToBecomeIndexed;

    public Directory(final DataBlocks dataBlocks, final int address, final int maxNameLen, final int directoryMinSizeToBecomeIndexed) throws IOException {
        checkNotNull(dataBlocks, "dataBlocks");
        checkArgument(address > 0, "address must be more than 0: %s", address);
        checkArgument(maxNameLen > 0, "max len must be more than 0 %s", maxNameLen);
        checkArgument(directoryMinSizeToBecomeIndexed > 0, "directory min size to become indexed must be more than 0 %s", directoryMinSizeToBecomeIndexed);

        this.dataBlocks = dataBlocks;
        this.rootBlock = dataBlocks.getBlock(address);
        this.lastEntityListInRootBlock = rootBlock.size() / PTRLEN - 1;
        this.maxNameLen = maxNameLen;
        this.directoryMinSizeToBecomeIndexed = directoryMinSizeToBecomeIndexed;

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
                .flatMap(entityList -> iteratorToStream(entityList.listEntities()))
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

    public DirectoryEntity find(final String name) throws IOException {
        checkNotNull(name, "name");

        final EntityList entityList = getEntityList(name, false);
        if (entityList == null) {
            return null;
        }

        final Iterator<DirectoryEntity> entityIterator = entityList.listEntities();
        while (entityIterator.hasNext()) {
            final DirectoryEntity entity = entityIterator.next();
            if (entity.getName().equals(name)) {
                return entity;
            }
        }

        return null;
    }

    public void addEntity(final String name, final int address, final Flags.DirectoryListEntityFlags flags) throws IOException {
        checkNotNull(name, "name");
        checkNotNull(flags, "flags");
        checkArgument(address > 0, "address must be more than 0: %s", address);
        checkArgument(name.length() <= maxNameLen, "name len must be less than %s: %s", maxNameLen, name);
        checkArgument(name.matches(NAME_REGEXP), "name doesn't match %s %s", NAME_REGEXP, name);

        checkState(find(name) == null, "element %s already exists", name);

        //noinspection ConstantConditions
        getEntityList(name, true).addEntity(name, address, flags);

        tryConvertToIndexed();
    }

    public void removeEntity(final String name) throws IOException {
        checkNotNull(name, "name");

        final EntityList entityList = getEntityList(name, false);
        if (entityList != null) {
            entityList.remove(name);

            if (entityList.size() == 0 && getFlags().isIndexed()) {
                final int listIdId = nameToListId(name);
                entityList.delete();
                rootBlock.writeInt(listIdId * PTRLEN, NULL_POINTER);

                log.debug("- entity list directory={} list={} address={}",
                        rootBlock.getAddress(), listIdId, entityList.entityListRootBlock.getAddress());
            }
        }
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

    private EntityList getEntityList(final String name, final boolean create) throws IOException {
        if (getFlags().isIndexed()) {
            final int entityListId = nameToListId(name);

            final int entityListBlockAddress = rootBlock.readInt(entityListId * PTRLEN);
            if (entityListBlockAddress != NULL_POINTER) {
                return new EntityList(entityListBlockAddress);
            }

            if (!create) {
                return null;
            }

            final DataBlocks.Block entityListBlock = dataBlocks.allocateBlock();
            entityListBlock.clear();
            rootBlock.writeInt(entityListId * PTRLEN, entityListBlock.getAddress());

            log.debug("+ entity list directory={} list={} address={}", rootBlock.getAddress(), entityListId, entityListBlock.getAddress());

            return new EntityList(entityListBlock.getAddress());
        } else {
            return new EntityList(rootBlock.readInt(FIRST_ENTITY_LIST_BLOCK_IDX * PTRLEN));
        }
    }

    private Flags.DirectoryFlags getFlags() throws IOException {
        return new Flags.DirectoryFlags(rootBlock.readInt(FLAGS_IDX * PTRLEN));
    }

    private void tryConvertToIndexed() throws IOException {
        final Flags.DirectoryFlags flags = getFlags();

        if (flags.isIndexed()) {
            return;
        }

        final EntityList singleEntityList = new EntityList(rootBlock.readInt(FIRST_ENTITY_LIST_BLOCK_IDX * PTRLEN));
        if (singleEntityList.size() < directoryMinSizeToBecomeIndexed) {
            return;
        }

        final Map<Integer, EntityList> entityListById = new HashMap<>();
        final Iterator<DirectoryEntity> entityIterator = singleEntityList.listEntities();

        while (entityIterator.hasNext()) {
            final DirectoryEntity entity = entityIterator.next();
            final EntityList entityList = entityListById.computeIfAbsent(nameToListId(entity.getName()), (id) -> {
                try {
                    final DataBlocks.Block entityListBlock = dataBlocks.allocateBlock();
                    entityListBlock.clear();
                    return new EntityList(entityListBlock.getAddress());
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            });

            entityList.addEntity(entity.getName(), entity.getAddress(), entity.getFlags());
        }

        singleEntityList.delete();

        final ByteBuffer rootBlockNewData = ByteBuffer.wrap(new byte[rootBlock.size()]);
        flags.setIndexed(true);
        rootBlockNewData.putInt(FLAGS_IDX * PTRLEN, flags.value());

        entityListById.forEach((id, list) -> rootBlockNewData.putInt(id * PTRLEN, list.entityListRootBlock.getAddress()));

        rootBlock.write(rootBlockNewData.array());

        log.debug("indexed {}", this);
    }

    private int nameToListId(final String name) {
        final MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance("SHA-256");
        } catch (final NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
        messageDigest.update(name.getBytes());
        final int nameHashCode = Math.abs(new String(messageDigest.digest()).hashCode());

        final int bucketSize = Integer.MAX_VALUE / (lastEntityListInRootBlock - FIRST_ENTITY_LIST_BLOCK_IDX);

        final int result = (int) Math.round (((double)nameHashCode / bucketSize) + FIRST_ENTITY_LIST_BLOCK_IDX);

        checkState(result >= FIRST_ENTITY_LIST_BLOCK_IDX && result <= lastEntityListInRootBlock,
                "unexpected block id %s for %s", result, name);
        return result;
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

        Iterator<DirectoryEntity> listEntities() {

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
                                if (nextListBlockAddress != NULL_POINTER) {
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
                if (nextListBlockAddress != NULL_POINTER) {
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
            Node node;
            DataBlocks.Block prevListBlock = null;
            DataBlocks.Block currentListBlock = entityListRootBlock;
            ByteBuffer currentListBlockData = ByteBuffer.wrap(currentListBlock.read());

            int addressInListBlock = FIRST_ENTITY_ADDRESS;
            while (true) {
                node = readNodeIfExists(currentListBlock.getAddress(), currentListBlockData, addressInListBlock);

                if (node == null) {
                    final int nextListBlockAddress = currentListBlockData.getInt(NEXT_BLOCK_IDX * PTRLEN);

                    if (nextListBlockAddress == NULL_POINTER) {
                        log.debug("not found for remove {} in {}", name, rootBlock.getAddress());
                        return;
                    }

                    prevListBlock = currentListBlock;

                    currentListBlock = dataBlocks.getBlock(nextListBlockAddress);
                    currentListBlockData = ByteBuffer.wrap(currentListBlock.read());
                    addressInListBlock = FIRST_ENTITY_ADDRESS;
                } else {
                    if (node.getName().equals(name)) {
                        break;
                    }

                    addressInListBlock += node.length() + 1;
                }
            }

            int addressInCurrentListBlock = FIRST_ENTITY_ADDRESS;
            int addressInNewListBlock = FIRST_ENTITY_ADDRESS;

            final ByteBuffer listBlockNewData = ByteBuffer.wrap(new byte[currentListBlock.size()]);

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

            if (prevListBlock == null || addressInNewListBlock != FIRST_ENTITY_ADDRESS) {
                listBlockNewData.putInt(SIZE_IDX * PTRLEN, currentListBlockData.getInt(SIZE_IDX * PTRLEN));
                listBlockNewData.putInt(NEXT_BLOCK_IDX * PTRLEN, currentListBlockData.getInt(NEXT_BLOCK_IDX * PTRLEN));
                currentListBlock.write(listBlockNewData.array());

                log.debug("- entity {} from list={} part={}", node.getAddress(), entityListRootBlock.getAddress(), currentListBlock.getAddress());
            } else {
                final int currentBlockNextBlockAddress = currentListBlockData.getInt(NEXT_BLOCK_IDX * PTRLEN);

                prevListBlock.writeInt(NEXT_BLOCK_IDX * PTRLEN, currentBlockNextBlockAddress);

                dataBlocks.deallocateBlock(currentListBlock.getAddress());
                log.debug("- entity {} with list part, list={} prev={} next={} gone={}",
                        node.getAddress(), entityListRootBlock.getAddress(), prevListBlock.getAddress(),
                        currentBlockNextBlockAddress, currentListBlock.getAddress());
            }

            entityListRootBlock.writeInt(SIZE_IDX * PTRLEN, size() - 1);
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
            final Iterator<DirectoryEntity> entityIterator = listEntities();
            while (entityIterator.hasNext()) {
                sb.append("  ").append(entityIterator.next()).append("\n");
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
                checkState(freeSpace >= length, "not enough space %s, %s", freeSpace, length);

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
