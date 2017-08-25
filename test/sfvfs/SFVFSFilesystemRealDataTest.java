package sfvfs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import sfvfs.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayDeque;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * @author alexey.kutuzov
 */
class SFVFSFilesystemRealDataTest {

    private Random r;

    @BeforeEach
    void setUp() {
        r = new Random();
        r.setSeed(0);
    }

    @Test
    void uploadProjectDir() throws IOException {
        final Path localfsPath = Paths.get(".");
        final File dataFile = createDataFile();

        final Path sfvfsRoot = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));
        uploadData(localfsPath, sfvfsRoot);

        checkData(localfsPath, sfvfsRoot, false);

        randomlyRemoveFiles(sfvfsRoot, 0.01, 0.7);

        checkData(localfsPath, sfvfsRoot, true);

        ((SFVFSFileSystem) sfvfsRoot.getFileSystem()).compact();

        assertTrue(((SFVFSFileSystem) sfvfsRoot.getFileSystem()).getFreeBlocks() <= SFVFSFilesystemProvider.BLOCK_SIZE);

        uploadData(localfsPath, sfvfsRoot.resolve("inner"));

        checkData(localfsPath, sfvfsRoot.resolve("inner"), true);

        sfvfsRoot.getFileSystem().close();

        final Path rootReopened = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));

        checkData(localfsPath, rootReopened, true);

        checkData(localfsPath, rootReopened.resolve("inner"), false);

        Files.move(rootReopened.resolve("inner"), rootReopened.resolve("inner2"));

        Files.copy(rootReopened.resolve("inner2"), rootReopened.resolve("inner3"));

        checkData(localfsPath, rootReopened.resolve("inner2"), false);

        checkData(localfsPath, rootReopened.resolve("inner3"), false);
    }

    private void uploadData(final Path localfsRootPath, final Path sfvfsRootPath) throws IOException {
        Files.createDirectory(sfvfsRootPath);

        Files.walk(localfsRootPath)
                .skip(1) //skip dir itself
                .forEach(localPath -> {
                    try {
                        final Path relativeLocalPath = localfsRootPath.relativize(localPath);
                        final Path sfvfsPath = sfvfsRootPath.resolve(relativeLocalPath.toString());

                        if (Files.isDirectory(localPath)) {
                            Files.createDirectory(sfvfsPath);
                            System.out.println("created dir " + relativeLocalPath);
                        } else {
                            try (final InputStream is = Files.newInputStream(localPath)) {
                                try (final OutputStream os = Files.newOutputStream(sfvfsPath, StandardOpenOption.CREATE_NEW)) {
                                    IOUtils.copy(is, os, 512);
                                }
                            }

                            System.out.println("created file " + relativeLocalPath);
                        }
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void checkData(final Path localfsRootPath, final Path sfvfsRootPath, final boolean skipNotExists) throws IOException {
        Files.walk(localfsRootPath)
                .skip(1) //skip dir itself
                .forEach(localPath -> {
                    final Path relativeLocalPath = localfsRootPath.relativize(localPath);

                    try {
                        final Path sfvfsPath = sfvfsRootPath.resolve(relativeLocalPath.toString());

                        final BasicFileAttributes localPathAttributes = Files.readAttributes(localPath, BasicFileAttributes.class);
                        final BasicFileAttributes sfvfsPathAttributes = Files.readAttributes(sfvfsPath, BasicFileAttributes.class);

                        //noinspection StatementWithEmptyBody
                        if (localPathAttributes.isDirectory() && sfvfsPathAttributes.isDirectory()) {
                            System.out.println("directory exists " + relativeLocalPath);
                        } else if (!localPathAttributes.isDirectory() && !sfvfsPathAttributes.isDirectory()) {
                            try (final InputStream localIs = Files.newInputStream(localPath)) {
                                try (final InputStream sfvfsIs = Files.newInputStream(sfvfsPath)) {
                                    assertTrue(IOUtils.isEqual(localIs, sfvfsIs, 1024), "streams different for " + relativeLocalPath);
                                }
                            }

                            System.out.println("file same content " + relativeLocalPath);
                        } else {
                            fail("different entity type " + relativeLocalPath);
                        }

                    } catch (final NoSuchFileException nfe) {
                        if (!skipNotExists) {
                            throw new RuntimeException(nfe);
                        } else {
                            System.out.println("file not exists, skipping " + relativeLocalPath);
                        }
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                });

    }

    @SuppressWarnings("SameParameterValue")
    private void randomlyRemoveFiles(final Path root, final double removeDirProbability, final double removeFileProbability) throws IOException {
        assertTrue(root.getFileSystem() instanceof SFVFSFileSystem);

        Files.walk(root)
                .skip(1) //skip dir itself
                .forEach(path -> {
                    try {
                        final BasicFileAttributes attributes = Files.readAttributes(path, BasicFileAttributes.class);
                        final double rValue = Math.abs(r.nextGaussian());

                        if (attributes.isDirectory()) {
                            if (rValue <= removeDirProbability) {
                                System.out.println("remove dir " + path);
                                doDelete(path, true);
                                doDelete(path, false);
                            }
                        } else {
                            if (rValue <= removeFileProbability) {
                                System.out.println("remove file " + path);
                                Files.delete(path);
                            }
                        }
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void doDelete(final Path path, final boolean deleteFiles) throws IOException {
        Files.walk(path)
                .collect(Collectors.toCollection(ArrayDeque::new)).descendingIterator() //reverse order
                .forEachRemaining(innerPath -> {
                    try {
                        final BasicFileAttributes innerAttributes = Files.readAttributes(path, BasicFileAttributes.class);
                        if (deleteFiles) {
                            if (innerAttributes.isRegularFile()) {
                                Files.delete(innerPath);
                            }
                        } else {
                            if (innerAttributes.isDirectory()) {
                                Files.delete(innerPath);
                            }
                        }
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private File createDataFile() throws IOException {
        final File dataFile = File.createTempFile("sfvfs", ".dat");
        assertTrue(dataFile.delete());
        dataFile.deleteOnExit();
        return dataFile;
    }

}
