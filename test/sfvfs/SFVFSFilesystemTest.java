package sfvfs;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author alexey.kutuzov
 */
class SFVFSFilesystemTest {

    private File dataFile;

    @BeforeEach
    void setUp() throws IOException {
        dataFile = File.createTempFile("sfvfs", ".dat");
        assertTrue(dataFile.delete());

        dataFile.deleteOnExit();
    }

    @Test
    void createRootPath() {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));
        assertTrue(Files.exists(path));
        assertTrue(Files.isDirectory(path));
    }

    @Test
    void nonExistingPathNotExists() {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/notextists"));
        assertFalse(Files.exists(path));
        assertFalse(Files.isDirectory(path));
    }

    @Test
    void createEmptyDir() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/new_dir/ab/asb/s"));
        assertFalse(Files.exists(path));
        assertFalse(Files.isDirectory(path));

        Files.createDirectory(path);

        assertTrue(Files.exists(path));
        assertTrue(Files.isDirectory(path));
    }

    @Test
    void createExistingDir() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/notextists"));

        Files.createDirectory(path);
        Files.createDirectory(path);

        assertTrue(Files.exists(path));
        assertTrue(Files.isDirectory(path));
    }

    @Test
    void deleteDir() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/notextists"));

        Files.createDirectory(path);
        Files.delete(path);

        assertFalse(Files.exists(path));
        assertFalse(Files.isDirectory(path));
    }

    @Test
    void deleteAndRecreateDir() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/notextists"));

        Files.createDirectory(path);
        Files.delete(path);
        Files.createDirectory(path);

        assertTrue(Files.exists(path));
        assertTrue(Files.isDirectory(path));
    }

    @Test
    void cantDeleteDirWithData() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/new_dir/ab/asb/s"));
        final Path parentDir = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/new_dir"));

        Files.createDirectory(path);
        try {
            Files.delete(parentDir);
            fail("IOException expected");
        } catch (final IOException ignore) {

        }
    }

    @Test
    void fileCreate() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/test.txt"));
        Files.write(path, "abc".getBytes(), StandardOpenOption.CREATE_NEW);
        assertEquals("abc", Files.readAllLines(path).get(0));
    }

    @Test
    void fileAppend() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/test.txt"));
        Files.write(path, "abc".getBytes(), StandardOpenOption.CREATE_NEW);
        Files.write(path, "d".getBytes(), StandardOpenOption.APPEND);
        assertEquals("abcd", Files.readAllLines(path).get(0));
    }

    @Test
    void fileOverwrite() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/test.txt"));
        Files.write(path, "abc".getBytes(), StandardOpenOption.CREATE_NEW);
        Files.write(path, "d".getBytes(), StandardOpenOption.WRITE);
        assertEquals("d", Files.readAllLines(path).get(0));
    }

    @Test
    void filesWalk() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/new_dir/ab/asb/s"));
        Files.createDirectory(path);

        final Path aTextFile = path.resolve("a.txt");
        Files.write(aTextFile, "abc".getBytes(), StandardOpenOption.CREATE_NEW);

        final Path innerDir = path.resolve("inner_dir");
        Files.createDirectory(innerDir);

        final Path bTextFile = innerDir.resolve("b.txt");
        Files.write(bTextFile, "abc".getBytes(), StandardOpenOption.CREATE_NEW);

        final List<String> entities = Files.walk(path.getRoot())
                .map(p -> ((SFVFSPath) p).getFSPath())
                .collect(Collectors.toList());

        final List<String> expected = new ArrayList<String>() {{
            add("/");
            add("/new_dir");
            add("/new_dir/ab");
            add("/new_dir/ab/asb");
            add("/new_dir/ab/asb/s");
            add("/new_dir/ab/asb/s/a.txt");
            add("/new_dir/ab/asb/s/inner_dir");
            add("/new_dir/ab/asb/s/inner_dir/b.txt");
        }};

        assertEquals(expected, entities);
    }

    @Test
    void moveFileDirectly() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));

        final Path aDir = path.resolve("a");
        final Path bDir = path.resolve("b");
        Files.createDirectory(aDir);
        Files.createDirectory(bDir);

        Files.write(aDir.resolve("f.txt"), "abc".getBytes(), StandardOpenOption.CREATE_NEW);

        Files.move(aDir.resolve("f.txt"), bDir.resolve("d.txt"));

        assertFalse(Files.exists(path.resolve("a/f.txt")));
        assertTrue(Files.exists(path.resolve("b/d.txt")));

        assertEquals("abc", Files.readAllLines(path.resolve("b/d.txt")).get(0));
    }

    @Test
    void moveFileToExistingDirectory() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));

        final Path aDir = path.resolve("a");
        final Path bDir = path.resolve("b");
        Files.createDirectory(aDir);
        Files.createDirectory(bDir);

        Files.write(aDir.resolve("f.txt"), "abc".getBytes(), StandardOpenOption.CREATE_NEW);

        Files.move(aDir.resolve("f.txt"), bDir);

        assertFalse(Files.exists(path.resolve("a/f.txt")));
        assertTrue(Files.exists(path.resolve("b/f.txt")));

        assertEquals("abc", Files.readAllLines(path.resolve("b/f.txt")).get(0));
    }

    @Test
    void cantMoveIfNoParent() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));

        final Path aDir = path.resolve("a");
        final Path bDir = path.resolve("b");
        Files.createDirectory(aDir);

        Files.write(aDir.resolve("f.txt"), "abc".getBytes(), StandardOpenOption.CREATE_NEW);

        try {
            Files.move(aDir.resolve("f.txt"), bDir.resolve("d.txt"));
            fail("parent not exists");
        } catch (final IOException ignored) {

        }
    }

    @Test
    void cantMoveIfNoSource() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));

        final Path aDir = path.resolve("a");
        final Path bDir = path.resolve("b");
        Files.createDirectory(aDir);
        Files.createDirectory(bDir);

        try {
            Files.move(aDir.resolve("f.txt"), bDir.resolve("d.txt"));
            fail("source not exists");
        } catch (final IOException ignored) {

        }
    }

    @Test
    void cantMoveIfAlreadyExists() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));

        final Path aDir = path.resolve("a");
        final Path bDir = path.resolve("b");
        Files.createDirectory(aDir);
        Files.createDirectory(bDir);

        Files.write(aDir.resolve("f.txt"), "abc".getBytes(), StandardOpenOption.CREATE_NEW);
        Files.write(bDir.resolve("d.txt"), "abc".getBytes(), StandardOpenOption.CREATE_NEW);

        try {
            Files.move(aDir.resolve("f.txt"), bDir.resolve("d.txt"));
            fail("already exists");
        } catch (final IOException ignored) {

        }
    }

    @Test
    void cantMoveIfNoSourceParent() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));

        final Path aDir = path.resolve("a");
        final Path bDir = path.resolve("b");
        Files.createDirectory(bDir);

        try {
            Files.move(aDir.resolve("f.txt"), bDir.resolve("d.txt"));
            fail("no parent source exists");
        } catch (final IOException ignored) {

        }
    }

    @Test
    void copyFile() throws IOException {
        final Path root = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/"));

        Files.write(root.resolve("a.txt"), "abc".getBytes(), StandardOpenOption.CREATE_NEW);
        Files.copy(root.resolve("a.txt"), root.resolve("b.txt"));

        assertEquals("abc", Files.readAllLines(root.resolve("a.txt")).get(0));
        assertEquals("abc", Files.readAllLines(root.resolve("b.txt")).get(0));

        Files.write(root.resolve("a.txt"), "d".getBytes(), StandardOpenOption.WRITE);

        assertEquals("d", Files.readAllLines(root.resolve("a.txt")).get(0));
        assertEquals("abc", Files.readAllLines(root.resolve("b.txt")).get(0));
    }

    @Test
    void copyDir() throws IOException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/origin_dir/ab/asb/s"));
        Files.createDirectory(path);

        final Path aTextFile = path.resolve("a.txt");
        Files.write(aTextFile, "abc".getBytes(), StandardOpenOption.CREATE_NEW);

        final Path innerDir = path.resolve("inner_dir");
        Files.createDirectory(innerDir);

        final Path bTextFile = innerDir.resolve("b.txt");
        Files.write(bTextFile, "abc".getBytes(), StandardOpenOption.CREATE_NEW);

        final Path originDir = path.getRoot().resolve("origin_dir");
        final Path targetDir = path.getRoot().resolve("target_dir");

        Files.copy(originDir, targetDir);

        final List<String> entities = Files.walk(path.getRoot())
                .map(p -> ((SFVFSPath) p).getFSPath())
                .collect(Collectors.toList());

        final List<String> expected = new ArrayList<String>() {{
            add("/");

            add("/origin_dir");
            add("/origin_dir/ab");
            add("/origin_dir/ab/asb");
            add("/origin_dir/ab/asb/s");
            add("/origin_dir/ab/asb/s/a.txt");
            add("/origin_dir/ab/asb/s/inner_dir");
            add("/origin_dir/ab/asb/s/inner_dir/b.txt");

            add("/target_dir");
            add("/target_dir/ab");
            add("/target_dir/ab/asb");
            add("/target_dir/ab/asb/s");
            add("/target_dir/ab/asb/s/a.txt");
            add("/target_dir/ab/asb/s/inner_dir");
            add("/target_dir/ab/asb/s/inner_dir/b.txt");
        }};

        assertEquals(expected, entities);
    }

    @Test
    void differentThreadAccessProhibited() throws IOException, InterruptedException {
        final Path path = Paths.get(URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/origin_dir"));

        final Future<?> result = Executors.newSingleThreadExecutor().submit(() -> {
            try {
                Files.createDirectory(path);
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }
        });

        try {
            result.get();
            fail("exception expected");
        } catch (final ExecutionException ignore) {
        }
    }

    @Test
    void closeAndOpenFS() throws IOException {
        final URI uri = URI.create("sfvfs:" + dataFile.getAbsolutePath() + ":/");

        final Path root = Paths.get(uri);
        Files.createDirectory(root.resolve("dir"));
        Files.write(root.resolve("dir/file.txt"), "abc".getBytes(), StandardOpenOption.CREATE_NEW);

        root.getFileSystem().close();

        final Path reopenedRoot = Paths.get(uri);

        assertTrue(Files.exists(reopenedRoot.resolve("dir")));
        assertEquals("abc", Files.readAllLines(reopenedRoot.resolve("dir/file.txt")).get(0));
    }

    @Test
    void passParamInUrl() throws IOException {
        final URI uri = URI.create("sfvfs:" + dataFile.getAbsolutePath() + "?dirMaxNameLen=5:/");
        final Path root = Paths.get(uri);

        try {
            Files.createDirectory(root.resolve("1234567890"));
            fail("dir name must be too long");
        } catch (final Exception ex) {
            assertTrue(ex.getMessage().contains("name len must be less than 5"));
        }

        Files.createDirectory(root.resolve("1234"));
    }

    @Test
    void passWrongParamInUrl() {
        try {
            final URI uri = URI.create("sfvfs:" + dataFile.getAbsolutePath() + "?dirMaxNameLen=0:/");
            final Path root = Paths.get(uri);

            fail("dir max name check must fail");
        } catch (final IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("max len must be more than 0"));
        }
    }

    @Test
    void passMultipleWrongParamsInUrl() {
        try {
            final URI uri = URI.create("sfvfs:" + dataFile.getAbsolutePath() + "?dirMaxNameLen=10&blockSize=12:/");
            final Path root = Paths.get(uri);

            fail("block size must fail");
        } catch (final IllegalArgumentException ex) {
            assertTrue(ex.getMessage().contains("block size must be power of 2"));
        }
    }


}