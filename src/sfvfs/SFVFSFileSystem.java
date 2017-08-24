package sfvfs;

import sfvfs.internal.DataBlocks;
import sfvfs.internal.Directory;
import sfvfs.internal.DirectoryEntity;
import sfvfs.internal.Flags;
import sfvfs.internal.Inode;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.nio.file.spi.FileSystemProvider;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Set;

import static sfvfs.utils.Preconditions.checkArgument;
import static sfvfs.utils.Preconditions.checkNotNull;
import static sfvfs.utils.Preconditions.checkState;

/**
 * @author alexey.kutuzov
 */
public class SFVFSFileSystem extends FileSystem {

    private final SFVFSFilesystemProvider provider;
    private final DataBlocks dataBlocks;
    private final SFVFSFileStore fileStore;

    private final int rootDirAddress;
    private final int dirMaxNameLen;
    private final int directoryMinSizeToBecomeIndexed;
    private final long workingThreadId;

    private boolean open = true;

    SFVFSFileSystem(final SFVFSFilesystemProvider provider, final DataBlocks dataBlocks, final int rootDirAddress, final int dirMaxNameLen, final int directoryMinSizeToBecomeIndexed) {
        checkNotNull(provider, "provider");
        checkNotNull(dataBlocks, "dataBlocks");
        checkArgument(rootDirAddress > 0, "root address must be > 0 %s", rootDirAddress);

        this.provider = provider;
        this.dataBlocks = dataBlocks;
        this.fileStore = new SFVFSFileStore(dataBlocks);

        this.rootDirAddress = rootDirAddress;
        this.workingThreadId = Thread.currentThread().getId();
        this.dirMaxNameLen = dirMaxNameLen;
        this.directoryMinSizeToBecomeIndexed = directoryMinSizeToBecomeIndexed;
    }

    @Override
    public FileSystemProvider provider() {
        return provider;
    }

    @Override
    public void close() throws IOException {
        checkThread();

        open = false;
        try {
            dataBlocks.close();
            provider.unRegisterFs(this);
        } catch (final Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return "/";
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        final ArrayList<Path> result = new ArrayList<>();
        result.add(new SFVFSPath(this, "/"));
        return result;
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        final ArrayList<FileStore> result = new ArrayList<>();
        result.add(fileStore);
        return result;
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return Collections.emptySet();
    }

    @Override
    public Path getPath(final String first, final String... more) {
        final StringBuilder sb = new StringBuilder();
        sb.append(first);
        for (final String aMore : more) {
            sb.append("/").append(aMore);
        }

        return new SFVFSPath(this, sb.toString());
    }

    @Override
    public PathMatcher getPathMatcher(final String syntaxAndPattern) {
        throw new UnsupportedOperationException();
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchService newWatchService() throws IOException {
        throw new UnsupportedOperationException();
    }

    DirectoryEntity getRootDirectoryEntity() {
        return new DirectoryEntity() {
            @Override
            public String getName() {
                return "/";
            }

            @Override
            public int getAddress() {
                return rootDirAddress;
            }

            @Override
            public boolean isDirectory() {
                return true;
            }

            @Override
            public int getParentDirectoryAddress() {
                return 0;
            }

            @Override
            public Flags.DirectoryListEntityFlags getFlags() {
                final Flags.DirectoryListEntityFlags flags = new Flags.DirectoryListEntityFlags();
                flags.setDirectory(true);
                return flags;
            }
        };
    }

    Directory getRootDirectory() throws IOException {
        checkThread();

        return getDirectory(rootDirAddress);
    }

    Directory getDirectory(final int address) throws IOException {
        checkThread();

        return new Directory(dataBlocks, address, dirMaxNameLen, directoryMinSizeToBecomeIndexed);
    }

    Directory createDirectory() throws IOException {
        checkThread();

        final DataBlocks.Block dirBlock = dataBlocks.allocateBlock();
        final Directory newDir = new Directory(dataBlocks, dirBlock.getAddress(), dirMaxNameLen, directoryMinSizeToBecomeIndexed);
        newDir.create();
        return newDir;
    }

    Inode getInode(final int address) throws IOException {
        checkThread();

        return new Inode(dataBlocks, address);
    }

    Inode createInode() throws IOException {
        checkThread();

        final DataBlocks.Block dirBlock = dataBlocks.allocateBlock();
        final Inode inode = new Inode(dataBlocks, dirBlock.getAddress());
        inode.clear();
        return inode;
    }

    private void checkThread() {
        checkState(Thread.currentThread().getId() == workingThreadId, "fs direct access by different than it's creator thread is prohibited, use thread with id %s", workingThreadId);
    }

}
