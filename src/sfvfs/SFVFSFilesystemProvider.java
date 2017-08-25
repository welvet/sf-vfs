package sfvfs;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sfvfs.internal.DataBlocks;
import sfvfs.internal.Directory;
import sfvfs.internal.DirectoryEntity;
import sfvfs.internal.Flags;
import sfvfs.internal.Inode;
import sfvfs.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.NonWritableChannelException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.AccessMode;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileTime;
import java.nio.file.spi.FileSystemProvider;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.Stack;

import static sfvfs.utils.Preconditions.checkArgument;
import static sfvfs.utils.Preconditions.checkState;

/**
 * @author alexey.kutuzov
 */
@SuppressWarnings("WeakerAccess")
public class SFVFSFilesystemProvider extends FileSystemProvider {

    static final int BLOCK_SIZE = 1024;
    static final int BLOCK_GROUPS_WITH_FREE_BLOCKS_CACHE_SIZE = 4;
    static final String MODE = "rw";
    static final int DIR_MAX_NAME_LEN = 255;
    static final int DIRECTORY_MIN_SIZE_TO_BECOME_INDEXED = 40;
    static final int MAX_BLOCKS = 1024 * 1024;
    static final int FREE_LOGICAL_ADDRESS_CACHE_SIZE = 1000;

    private static final int ROOT_DATA_BLOCK_ADDRESS = 1;

    private static final Logger log = LoggerFactory.getLogger(SFVFSFilesystemProvider.class);

    private final Map<String, FileSystem> fileSystems = new HashMap<>();

    @Override
    public String getScheme() {
        return "sfvfs";
    }

    @Override
    public FileSystem newFileSystem(final URI uri, final Map<String, ?> env) throws IOException {
        synchronized (fileSystems) {
            final String dataFilePath = extractDataFile(uri);
            checkState(!fileSystems.containsKey(dataFilePath), "fs already exists %s", uri);

            final File dataFile = new File(dataFilePath);
            final boolean createNew = !dataFile.exists();

            if (createNew) {
                checkState(dataFile.createNewFile(), "can't create file %s", dataFile.getAbsolutePath());
            }

            final DataBlocks dataBlocks = new DataBlocks(dataFile, BLOCK_SIZE, BLOCK_GROUPS_WITH_FREE_BLOCKS_CACHE_SIZE, MODE, MAX_BLOCKS, FREE_LOGICAL_ADDRESS_CACHE_SIZE);
            if (createNew) {
                final DataBlocks.Block rootDirBlock = dataBlocks.allocateBlock();
                checkState(rootDirBlock.getAddress() == ROOT_DATA_BLOCK_ADDRESS, "root block must have address 1");
                final Directory rootDirectory = new Directory(dataBlocks, rootDirBlock.getAddress(), DIR_MAX_NAME_LEN, DIRECTORY_MIN_SIZE_TO_BECOME_INDEXED);
                rootDirectory.create();
            }

            final SFVFSFileSystem fileSystem = new SFVFSFileSystem(this, dataBlocks, ROOT_DATA_BLOCK_ADDRESS, DIR_MAX_NAME_LEN, DIRECTORY_MIN_SIZE_TO_BECOME_INDEXED);
            fileSystems.put(dataFilePath, fileSystem);

            return fileSystem;
        }
    }

    @Override
    public FileSystem getFileSystem(final URI uri) {
        synchronized (fileSystems) {
            final String dataFilePath = extractDataFile(uri);
            final FileSystem fileSystem = fileSystems.get(dataFilePath);
            if (fileSystem != null) {
                return fileSystem;
            }

            try {
                return newFileSystem(uri, Collections.emptyMap());
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public Path getPath(final URI uri) {
        final String schemeSpecificPart = uri.getSchemeSpecificPart();
        final FileSystem fileSystem = getFileSystem(uri);

        final String fsPath = schemeSpecificPart.substring(schemeSpecificPart.indexOf(':') + 1);
        log.debug("get path {}", fsPath);
        return fileSystem.getPath(fsPath);
    }

    @Override
    public SeekableByteChannel newByteChannel(final Path path, final Set<? extends OpenOption> options, final FileAttribute<?>... attrs) throws IOException {
        final SFVFSPath sfvfsPath = (SFVFSPath) path;
        final SFVFSFileSystem sfvfsFileSystem = sfvfsPath.getFileSystem();

        if (options.contains(StandardOpenOption.DELETE_ON_CLOSE)
                || options.contains(StandardOpenOption.SPARSE)
                || options.contains(StandardOpenOption.SYNC)
                || options.contains(StandardOpenOption.DSYNC)) {
            throw new UnsupportedOperationException("one of options is not supported " + options);
        }

        if (options.contains(StandardOpenOption.WRITE) || options.contains(StandardOpenOption.APPEND)) {
            return writeChannel(sfvfsPath, sfvfsFileSystem, options);
        }

        return readChannel(sfvfsPath, sfvfsFileSystem, options);
    }

    @Override
    public DirectoryStream<Path> newDirectoryStream(final Path dir, final DirectoryStream.Filter<? super Path> filter) throws IOException {
        final SFVFSPath sfvfsPath = (SFVFSPath) dir;
        final SFVFSFileSystem sfvfsFileSystem = sfvfsPath.getFileSystem();

        final DirectoryEntity directoryEntity = findEntity(sfvfsFileSystem, sfvfsPath.getFSPath());
        if (directoryEntity == null || !directoryEntity.isDirectory()) {
            throw new IOException("directory not exists " + ((SFVFSPath) dir).getFSPath());
        }

        final Directory directory = sfvfsFileSystem.getDirectory(directoryEntity.getAddress());
        final Iterator<DirectoryEntity> iter = directory.listEntities();

        return new DirectoryStream<Path>() {

            private boolean closed;
            private boolean iterReturned;

            @Override
            public Iterator<Path> iterator() {

                checkState(!closed, "closed");
                checkState(!iterReturned, "iterator already returned");
                iterReturned = false;

                return new Iterator<Path>() {
                    private Path next;

                    @Override
                    public boolean hasNext() {
                        checkState(!closed, "closed");

                        if (next != null) {
                            return true;
                        }

                        while (iter.hasNext()) {
                            final DirectoryEntity nextDirEntity = iter.next();
                            final Path nextPath = sfvfsPath.resolve(nextDirEntity.getName());
                            try {
                                if (filter.accept(nextPath)) {
                                    this.next = nextPath;
                                    break;
                                }
                            } catch (final IOException e) {
                                throw new RuntimeException(e);
                            }
                        }

                        return next != null;
                    }

                    @Override
                    public Path next() {
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

            @Override
            public void close() throws IOException {
                closed = true;
            }
        };
    }

    @Override
    public void createDirectory(final Path path, final FileAttribute<?>... attrs) throws IOException {
        final SFVFSPath sfvfsPath = (SFVFSPath) path;
        final SFVFSFileSystem sfvfsFileSystem = sfvfsPath.getFileSystem();

        final Stack<String> paths = new Stack<>();

        SFVFSPath current = sfvfsPath;
        while (current != null) {
            paths.push(current.getFSName());
            current = current.getParent();
        }

        paths.pop(); //remove root
        Directory curDir = sfvfsFileSystem.getRootDirectory();

        while (!paths.isEmpty()) {
            final String fsPathPart = paths.pop();
            final DirectoryEntity entity = curDir.find(fsPathPart);

            if (entity == null) {
                final Directory newDir = sfvfsFileSystem.createDirectory();
                final Flags.DirectoryListEntityFlags newDirFlags = new Flags.DirectoryListEntityFlags();
                newDirFlags.setDirectory(true);
                curDir.addEntity(fsPathPart, newDir.getRootBlockAddress(), newDirFlags);

                log.info("dir created par={} name={} cur={}", curDir.getRootBlockAddress(), fsPathPart, newDir.getRootBlockAddress());

                curDir = newDir;
            } else {
                checkState(entity.isDirectory(), "is not a dir %s", fsPathPart);
                curDir = sfvfsFileSystem.getDirectory(entity.getAddress());
            }
        }
    }

    @Override
    public void delete(final Path path) throws IOException {
        final SFVFSPath sfvfsPath = (SFVFSPath) path;
        final SFVFSFileSystem sfvfsFileSystem = sfvfsPath.getFileSystem();

        final DirectoryEntity entity = findEntity(sfvfsFileSystem, sfvfsPath.getFSPath());
        if (entity == null) {
            throw new NoSuchFileException("not exists " + sfvfsPath.getFSPath());
        }

        if (entity.isDirectory()) {
            final Directory directory = sfvfsFileSystem.getDirectory(entity.getAddress());
            directory.delete();
        } else {
            final Inode inode = sfvfsFileSystem.getInode(entity.getAddress());
            inode.delete();
        }

        final Directory parentDirectory = sfvfsFileSystem.getDirectory(entity.getParentDirectoryAddress());
        parentDirectory.removeEntity(entity.getName());

        log.info("removed {}", sfvfsPath.getFSPath());
    }

    @Override
    public void copy(final Path source, final Path target, final CopyOption... options) throws IOException {
        checkArgument(Arrays.stream(options).noneMatch(o -> o == StandardCopyOption.REPLACE_EXISTING), "replacing not supported");

        final SFVFSPath sfvfsSourcePath = (SFVFSPath) source;
        final SFVFSPath sfvfsTargetPath = (SFVFSPath) target;
        checkArgument(sfvfsSourcePath.getFileSystem() == sfvfsTargetPath.getFileSystem(), "different fs");

        log.info("copy {} {}", sfvfsSourcePath.getFSPath(), sfvfsTargetPath.getFSPath());

        final SFVFSFileSystem sfvfsFileSystem = sfvfsSourcePath.getFileSystem();

        final SourceToTarget st = computeSourceToTargetForMoveAndCopy(sfvfsFileSystem, sfvfsSourcePath, sfvfsTargetPath);
        copyEntity(sfvfsFileSystem, st);
    }

    @Override
    public void move(final Path source, final Path target, final CopyOption... options) throws IOException {
        checkArgument(Arrays.stream(options).noneMatch(o -> o == StandardCopyOption.REPLACE_EXISTING), "replacing not supported");

        final SFVFSPath sfvfsSourcePath = (SFVFSPath) source;
        final SFVFSPath sfvfsTargetPath = (SFVFSPath) target;
        checkArgument(sfvfsSourcePath.getFileSystem() == sfvfsTargetPath.getFileSystem(), "different fs");

        log.info("move {} {}", sfvfsSourcePath.getFSPath(), sfvfsTargetPath.getFSPath());

        final SFVFSFileSystem sfvfsFileSystem = sfvfsSourcePath.getFileSystem();

        final SourceToTarget st = computeSourceToTargetForMoveAndCopy(sfvfsFileSystem, sfvfsSourcePath, sfvfsTargetPath);

        st.sourceParentDirectory.removeEntity(st.sourceEntry.getName());
        st.resolvedTargetDir.addEntity(st.resolvedTargetName, st.sourceEntry.getAddress(), st.sourceEntry.getFlags());

        log.info("moved {} {} from={} to={} name={}",
                st.sourceEntry.getName(), st.sourceEntry.getAddress(), st.sourceParentDirectory.getRootBlockAddress(),
                st.resolvedTargetDir.getRootBlockAddress(), st.resolvedTargetName);
    }

    @Override
    public boolean isSameFile(final Path path, final Path path2) throws IOException {
        return path.toAbsolutePath().equals(path2.toAbsolutePath());
    }

    @Override
    public boolean isHidden(final Path path) throws IOException {
        return false;
    }

    @Override
    public FileStore getFileStore(final Path path) throws IOException {
        return path.getFileSystem().getFileStores().iterator().next();
    }

    @Override
    public void checkAccess(final Path path, final AccessMode... modes) throws IOException {
        final SFVFSPath sfvfsPath = (SFVFSPath) path;
        final SFVFSFileSystem sfvfsFileSystem = sfvfsPath.getFileSystem();

        if (sfvfsPath.getFSPath().equals("/")) {
            return;
        }

        final DirectoryEntity entity = findEntity(sfvfsFileSystem, sfvfsPath.getFSPath());
        if (entity == null) {
            throw new NoSuchFileException("not exists " + sfvfsPath.getFSPath());
        }
    }

    @Override
    public <V extends FileAttributeView> V getFileAttributeView(final Path path, final Class<V> type, final LinkOption... options) {
        throw new UnsupportedOperationException("attributes not supported");
    }

    @Override
    public <A extends BasicFileAttributes> A readAttributes(final Path path, final Class<A> type, final LinkOption... options) throws IOException {
        checkArgument(type == BasicFileAttributes.class, "only BasicFileAttributes supported");
        final SFVFSPath sfvfsPath = (SFVFSPath) path;
        final SFVFSFileSystem sfvfsFileSystem = sfvfsPath.getFileSystem();

        if (((SFVFSPath) path).getFSPath().equals("/")) {
            //noinspection unchecked
            return (A) new SFVFSBasicAttributes(true, sfvfsFileSystem.getRootDirectory().size());
        }

        final DirectoryEntity entity = findEntity(sfvfsFileSystem, sfvfsPath.getFSPath());
        if (entity == null) {
            throw new NoSuchFileException("not exists " + sfvfsPath.getFSPath());
        }

        if (entity.isDirectory()) {
            final int size = sfvfsFileSystem.getDirectory(entity.getAddress()).size();
            //noinspection unchecked
            return (A) new SFVFSBasicAttributes(true, size);
        } else {
            final int size = sfvfsFileSystem.getInode(entity.getAddress()).getSize();
            //noinspection unchecked
            return (A) new SFVFSBasicAttributes(false, size);
        }
    }

    @Override
    public Map<String, Object> readAttributes(final Path path, final String attributes, final LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("attributes not supported");
    }

    @Override
    public void setAttribute(final Path path, final String attribute, final Object value, final LinkOption... options) throws IOException {
        throw new UnsupportedOperationException("attributes not supported");
    }

    private String extractDataFile(final URI uri) {
        final String schemeSpecificPart = uri.getSchemeSpecificPart();
        final int delimIdx = schemeSpecificPart.indexOf(':');
        checkArgument(delimIdx > 0, "not found ':' delimiter");
        return schemeSpecificPart.substring(0, delimIdx).toLowerCase();
    }

    private SourceToTarget computeSourceToTargetForMoveAndCopy(final SFVFSFileSystem sfvfsFileSystem, final SFVFSPath sfvfsSourcePath, final SFVFSPath sfvfsTargetPath) throws IOException {
        final DirectoryEntity sourceParentEntry = findEntity(sfvfsFileSystem, sfvfsSourcePath.getParent().getFSPath());
        final DirectoryEntity targetParentEntry = findEntity(sfvfsFileSystem, sfvfsTargetPath.getParent().getFSPath());

        if (sourceParentEntry == null || !sourceParentEntry.isDirectory()) {
            throw new IOException("source parent is not a directory");
        }

        if (targetParentEntry == null || !targetParentEntry.isDirectory()) {
            throw new IOException("target parent is not a directory");
        }

        final Directory sourceParentDirectory = sfvfsFileSystem.getDirectory(sourceParentEntry.getAddress());
        final Directory targetParentDirectory = sfvfsFileSystem.getDirectory(targetParentEntry.getAddress());

        final DirectoryEntity sourceEntry = sourceParentDirectory.find(sfvfsSourcePath.getFSName());
        final DirectoryEntity targetEntry = targetParentDirectory.find(sfvfsTargetPath.getFSName());

        if (sourceEntry == null) {
            throw new IOException("source not exists");
        }

        final Directory resolvedTargetDir;
        final String resolvedTargetName;
        if (targetEntry != null) {
            if (!targetEntry.isDirectory()) {
                throw new IOException("target already exists");
            }

            resolvedTargetDir = sfvfsFileSystem.getDirectory(targetEntry.getAddress());
            resolvedTargetName = sfvfsSourcePath.getFSName();
        } else {
            resolvedTargetDir = targetParentDirectory;
            resolvedTargetName = sfvfsTargetPath.getFSName();
        }

        if (resolvedTargetDir.find(resolvedTargetName) != null) {
            throw new IOException("name already exists in target " + resolvedTargetName);
        }

        return new SourceToTarget(sourceParentDirectory, sourceEntry, resolvedTargetDir, resolvedTargetName);
    }

    private void copyEntity(final SFVFSFileSystem sfvfsFileSystem, final SourceToTarget st) throws IOException {
        if (st.sourceEntry.isDirectory()) {
            final Directory newDirectory = sfvfsFileSystem.createDirectory();
            st.resolvedTargetDir.addEntity(st.resolvedTargetName, newDirectory.getRootBlockAddress(), st.sourceEntry.getFlags());

            log.info("copied dir {} {} from={} to={} name={}",
                    st.sourceEntry.getName(), st.sourceEntry.getAddress(), st.sourceParentDirectory.getRootBlockAddress(),
                    st.resolvedTargetDir.getRootBlockAddress(), st.resolvedTargetName);

            final Directory source = sfvfsFileSystem.getDirectory(st.sourceEntry.getAddress());
            final Iterator<DirectoryEntity> entityIterator = source.listEntities();
            while (entityIterator.hasNext()) {
                final DirectoryEntity entityToCopy = entityIterator.next();
                copyEntity(sfvfsFileSystem, new SourceToTarget(source, entityToCopy, newDirectory, entityToCopy.getName()));
            }
        } else {
            final Inode newInode = sfvfsFileSystem.createInode();
            newInode.clear();
            st.resolvedTargetDir.addEntity(st.resolvedTargetName, newInode.getAddress(), new Flags.DirectoryListEntityFlags());

            final Inode source = sfvfsFileSystem.getInode(st.sourceEntry.getAddress());
            try (final InputStream readStream = source.readStream()) {
                try(final OutputStream appendStream = newInode.appendStream()) {
                    IOUtils.copy(readStream, appendStream, BLOCK_SIZE);
                }
            }

            log.info("copied inode {} {} from={} to={} name={}",
                    st.sourceEntry.getName(), st.sourceEntry.getAddress(), st.sourceParentDirectory.getRootBlockAddress(),
                    st.resolvedTargetDir.getRootBlockAddress(), st.resolvedTargetName);
        }
    }

    void unRegisterFs(final SFVFSFileSystem fileSystem) {
        checkState(!fileSystem.isOpen(), "system still open");
        synchronized (fileSystems) {
            String key = null;
            for (final Map.Entry<String, FileSystem> fileSystemEntry : fileSystems.entrySet()) {
                if (fileSystemEntry.getValue() == fileSystem) {
                    key = fileSystemEntry.getKey();
                    break;
                }
            }

            if (key != null) {
                log.info("fs unregistered {}", key);
                fileSystems.remove(key);
            }
        }
    }

    private class SourceToTarget {
        final Directory sourceParentDirectory;
        final DirectoryEntity sourceEntry;
        final Directory resolvedTargetDir;
        final String resolvedTargetName;

        private SourceToTarget(final Directory sourceParentDirectory, final DirectoryEntity sourceEntry, final Directory resolvedTargetDir, final String resolvedTargetName) {
            this.sourceParentDirectory = sourceParentDirectory;
            this.sourceEntry = sourceEntry;
            this.resolvedTargetDir = resolvedTargetDir;
            this.resolvedTargetName = resolvedTargetName;
        }
    }

    private class SFVFSBasicAttributes implements BasicFileAttributes {

        private final boolean directory;
        private final long size;

        private SFVFSBasicAttributes(final boolean directory, final long size) {
            this.directory = directory;
            this.size = size;
        }

        @Override
        public FileTime lastModifiedTime() {
            return null;
        }

        @Override
        public FileTime lastAccessTime() {
            return null;
        }

        @Override
        public FileTime creationTime() {
            return null;
        }

        @Override
        public boolean isRegularFile() {
            return !directory;
        }

        @Override
        public boolean isDirectory() {
            return directory;
        }

        @Override
        public boolean isSymbolicLink() {
            return false;
        }

        @Override
        public boolean isOther() {
            return false;
        }

        @Override
        public long size() {
            return size;
        }

        @Override
        public Object fileKey() {
            return null;
        }

    }

    private DirectoryEntity findEntity(final SFVFSFileSystem fs, final String path) throws IOException {
        if (path.equals("/")) {
            return fs.getRootDirectoryEntity();
        }

        final String[] parts = path.split("/");

        Directory directory = fs.getRootDirectory();
        DirectoryEntity currentEntity = null;

        for (int i = 1; i < parts.length; i++) {
            final String part = parts[i];
            if (directory == null) {
                return null;
            }

            currentEntity = directory.find(part);
            if (currentEntity == null) {
                return null;
            }

            if (currentEntity.isDirectory()) {
                directory = fs.getDirectory(currentEntity.getAddress());
            }
        }

        return currentEntity;
    }

    private SeekableByteChannel readChannel(final SFVFSPath sfvfsPath, final SFVFSFileSystem sfvfsFileSystem, final Set<? extends OpenOption> options) throws IOException {
        final DirectoryEntity entity = findEntity(sfvfsFileSystem, sfvfsPath.getFSPath());

        if (entity == null) {
            throw new NoSuchFileException("not exists " + sfvfsPath.getFSPath());
        }

        if (entity.isDirectory()) {
            throw new IOException(sfvfsPath.getFSPath() + " is dir");
        }

        final Inode inode = sfvfsFileSystem.getInode(entity.getAddress());

        final ReadableByteChannel rbc = Channels.newChannel(inode.readStream());
        final long size = inode.getSize();

        log.debug("new read channel {}", sfvfsPath.getFSPath());

        return new SeekableByteChannel() {

            private long read = 0;

            public boolean isOpen() {
                return rbc.isOpen();
            }

            public long position() throws IOException {
                return read;
            }

            public SeekableByteChannel position(final long pos)
                    throws IOException {
                throw new UnsupportedOperationException();
            }

            public int read(final ByteBuffer dst) throws IOException {
                final int n = rbc.read(dst);
                read += n;
                return n;
            }

            public SeekableByteChannel truncate(final long size) throws IOException {
                throw new NonWritableChannelException();
            }

            public int write(final ByteBuffer src) throws IOException {
                throw new NonWritableChannelException();
            }

            public long size() throws IOException {
                return size;
            }

            public void close() throws IOException {
                rbc.close();
            }
        };
    }

    private SeekableByteChannel writeChannel(final SFVFSPath sfvfsPath, final SFVFSFileSystem sfvfsFileSystem, final Set<? extends OpenOption> options) throws IOException {
        final String parentPath = sfvfsPath.getParent().getFSPath();
        final DirectoryEntity parentDirEntry = findEntity(sfvfsFileSystem, parentPath);

        if (parentDirEntry == null || !parentDirEntry.isDirectory()) {
            throw new IOException("parent dir not exists " + parentPath);
        }

        final Directory parentDir = sfvfsFileSystem.getDirectory(parentDirEntry.getAddress());
        final DirectoryEntity entity = parentDir.find(sfvfsPath.getFSName());

        if (entity == null) {
            if (!(options.contains(StandardOpenOption.CREATE_NEW) || !options.contains(StandardOpenOption.CREATE))) {
                throw new NoSuchFileException("file not exists " + sfvfsPath.getFSPath());
            }
        } else {
            if (options.contains(StandardOpenOption.CREATE_NEW)) {
                throw new NoSuchFileException("file already exists");
            }
        }

        final Inode inode;
        if (entity == null) {
            final Inode newInode = sfvfsFileSystem.createInode();

            parentDir.addEntity(sfvfsPath.getFSName(), newInode.getAddress(), new Flags.DirectoryListEntityFlags());
            inode = newInode;

            log.info("inode created {} {}", sfvfsPath.getFSPath(), inode.getAddress());
        } else {
            inode = sfvfsFileSystem.getInode(entity.getAddress());
        }

        if (!options.contains(StandardOpenOption.APPEND)) {
            inode.clear();
        }

        log.debug("new write channel {}", sfvfsPath.getFSPath());

        final WritableByteChannel wbc = Channels.newChannel(inode.appendStream());
        return new SeekableByteChannel() {

            private long written = size();

            public boolean isOpen() {
                return wbc.isOpen();
            }

            public long position() throws IOException {
                return written;
            }

            public SeekableByteChannel position(final long pos) throws IOException {
                throw new UnsupportedOperationException();
            }

            public int read(final ByteBuffer dst) throws IOException {
                throw new UnsupportedOperationException();
            }

            public SeekableByteChannel truncate(final long size) throws IOException {
                throw new UnsupportedOperationException();
            }

            public int write(final ByteBuffer src) throws IOException {
                final int n = wbc.write(src);
                written += n;
                return n;
            }

            public long size() throws IOException {
                return written;
            }

            public void close() throws IOException {
                wbc.close();
            }
        };
    }

}
