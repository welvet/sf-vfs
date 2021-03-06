package sfvfs;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sfvfs.utils.IOUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Random;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * @author alexey.kutuzov
 */
public class SFVFSFilesystemRealDataTest {

    private static final Logger log = LoggerFactory.getLogger(SFVFSFilesystemRealDataTest.class);

    private Random r;

    @Before
    public void setUp() {
        r = new Random();
        r.setSeed(0);
    }

    @Test
    public void uploadProjectDir() throws IOException {
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

    @Test
    @Ignore
    public void pTest() throws IOException {
        System.out.println("mem: " + Runtime.getRuntime().maxMemory());
        System.out.println();

        for (int i = 0; i < 3; i++) {
            final Path localfsPath = Paths.get("/Users/alexey.kutuzov/software/backend/modules/persist");
            final File dataFile = createDataFile();

            final long l = System.currentTimeMillis();

            final Path sfvfsRoot = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));

            uploadData(localfsPath, sfvfsRoot.resolve("inner"));

            Files.copy(sfvfsRoot.resolve("inner"), sfvfsRoot.resolve("inner2"));

            randomlyRemoveFiles(sfvfsRoot.resolve("inner"), Double.MAX_VALUE, Double.MAX_VALUE);

            Files.delete(sfvfsRoot.resolve("inner"));

            ((SFVFSFileSystem) sfvfsRoot.getFileSystem()).compact();

            checkData(localfsPath, sfvfsRoot.resolve("inner2"), false);

            sfvfsRoot.getFileSystem().close();

            System.out.println("len " + dataFile.length());
            System.out.println("time " + (System.currentTimeMillis() - l));
            System.out.println();

            //noinspection ResultOfMethodCallIgnored
            dataFile.delete();
        }
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
                            log.info("created dir {}", relativeLocalPath);
                        } else {
                            try (final InputStream is = Files.newInputStream(localPath)) {
                                try (final OutputStream os = Files.newOutputStream(sfvfsPath, StandardOpenOption.CREATE_NEW)) {
                                    IOUtils.copy(is, os, 512);
                                }
                            }

                            log.info("created file {}", relativeLocalPath);
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
                            log.info("directory exists {}", relativeLocalPath);
                        } else if (!localPathAttributes.isDirectory() && !sfvfsPathAttributes.isDirectory()) {
                            try (final InputStream localIs = Files.newInputStream(localPath)) {
                                try (final InputStream sfvfsIs = Files.newInputStream(sfvfsPath)) {
                                    assertTrue("streams different for " + relativeLocalPath, IOUtils.isEqual(localIs, sfvfsIs, 1024));
                                }
                            }

                            log.info("file same content {}", relativeLocalPath);
                        } else {
                            fail("different entity type " + relativeLocalPath);
                        }

                    } catch (final NoSuchFileException nfe) {
                        if (!skipNotExists) {
                            throw new RuntimeException(nfe);
                        } else {
                            log.info("file not exists, skipping {}", relativeLocalPath);
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
                                log.info("remove dir {}", path);
                                doDelete(path);
                            }
                        } else {
                            if (rValue <= removeFileProbability) {
                                log.info("remove file {}", path);
                                Files.delete(path);
                            }
                        }
                    } catch (final IOException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private void doDelete(final Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(final Path dir, final IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
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
