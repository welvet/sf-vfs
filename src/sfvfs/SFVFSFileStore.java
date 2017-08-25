package sfvfs;

import sfvfs.internal.DataBlocks;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;

import static sfvfs.utils.Preconditions.checkNotNull;

/**
 * @author alexey.kutuzov
 */
@SuppressWarnings("WeakerAccess")
public class SFVFSFileStore extends FileStore {

    private final DataBlocks dataBlocks;

    SFVFSFileStore(final DataBlocks dataBlocks) {
        checkNotNull(dataBlocks, "dataBlocks");
        this.dataBlocks = dataBlocks;
    }

    @Override
    public String name() {
        return "main";
    }

    @Override
    public String type() {
        return "main";
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public long getTotalSpace() throws IOException {
        return (dataBlocks.getTotalBlocks() * dataBlocks.getFreeBlocks()) * dataBlocks.getBlockSize();
    }

    @Override
    public long getUsableSpace() throws IOException {
        return (dataBlocks.getTotalBlocks() - dataBlocks.getFreeBlocks()) * dataBlocks.getBlockSize();
    }

    @Override
    public long getUnallocatedSpace() throws IOException {
        return dataBlocks.getFreeBlocks() * dataBlocks.getBlockSize();
    }

    @Override
    public boolean supportsFileAttributeView(final Class<? extends FileAttributeView> type) {
        return false;
    }

    @Override
    public boolean supportsFileAttributeView(final String name) {
        return false;
    }

    @Override
    public <V extends FileStoreAttributeView> V getFileStoreAttributeView(final Class<V> type) {
        return null;
    }

    @Override
    public Object getAttribute(final String attribute) throws IOException {
        return null;
    }

}
