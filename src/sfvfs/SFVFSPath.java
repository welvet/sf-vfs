package sfvfs;

import sfvfs.internal.DataBlocks;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.Iterator;

import static sfvfs.internal.Directory.NAME_REGEXP_AND_SLASH;
import static sfvfs.utils.Preconditions.checkArgument;
import static sfvfs.utils.Preconditions.checkNotNull;

/**
 * @author alexey.kutuzov
 */
public class SFVFSPath implements Path {
    private final SFVFSFileSystem sfvfsFileSystem;
    private final String path;

    SFVFSPath(final SFVFSFileSystem sfvfsFileSystem, final String path) {
        checkNotNull(sfvfsFileSystem, "sfvfsFileSystem");
        checkNotNull(path, "path");
        checkArgument(path.startsWith("/"), "path must start with / %s", path);
        checkArgument(path.matches(NAME_REGEXP_AND_SLASH), "path doesn't match %s %s", NAME_REGEXP_AND_SLASH, path);

        this.sfvfsFileSystem = sfvfsFileSystem;
        try {
            this.path = new URI(path).normalize().getPath();
        } catch (final URISyntaxException e) {
            throw new RuntimeException(e);
        }

        checkArgument(!this.path.isEmpty(), "path is empty after normalization %s", path);
    }

    @Override
    public SFVFSFileSystem getFileSystem() {
        return sfvfsFileSystem;
    }

    @Override
    public boolean isAbsolute() {
        return true;
    }

    @Override
    public SFVFSPath getRoot() {
        return new SFVFSPath(sfvfsFileSystem, "/");
    }

    @Override
    public Path getFileName() {
        throw new UnsupportedOperationException();
    }

    @Override
    public SFVFSPath getParent() {
        if (path.equals("/")) {
            return null;
        }

        final int lastDelimIndex = path.lastIndexOf("/");
        if (lastDelimIndex == 0) {
            return getRoot();
        }

        final String substring = path.substring(0, lastDelimIndex);
        return new SFVFSPath(sfvfsFileSystem, substring);
    }

    @Override
    public int getNameCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path getName(final int index) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path subpath(final int beginIndex, final int endIndex) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean startsWith(final Path other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean startsWith(final String other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean endsWith(final Path other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean endsWith(final String other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path normalize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path resolve(final Path other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path resolve(final String other) {
        final String newPath;
        if (this.path.equals("/")) {
            newPath = "/" + other;
        } else {
            newPath = path + "/" + other;
        }

        return new SFVFSPath(sfvfsFileSystem, newPath);
    }

    @Override
    public Path resolveSibling(final Path other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path resolveSibling(final String other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path relativize(final Path other) {
        throw new UnsupportedOperationException();
    }

    @Override
    public URI toUri() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Path toAbsolutePath() {
        return this;
    }

    @Override
    public Path toRealPath(final LinkOption... options) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(final WatchService watcher, final WatchEvent.Kind<?>[] events, final WatchEvent.Modifier... modifiers) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchKey register(final WatchService watcher, final WatchEvent.Kind<?>... events) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Iterator<Path> iterator() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int compareTo(final Path other) {
        final SFVFSPath otherPath = (SFVFSPath) other;
        return path.compareTo(otherPath.path);
    }

    String getFSPath() {
        return path;
    }

    String getFSName() {
        if (path.equals("/")) {
            return null;
        }

        final int lastDelimIndex = path.lastIndexOf("/");
        return path.substring(lastDelimIndex + 1);
    }

    @Override
    public String toString() {
        return path;
    }
}
