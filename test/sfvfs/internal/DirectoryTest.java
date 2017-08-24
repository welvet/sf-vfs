package sfvfs.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;
import static sfvfs.utils.Streams.iteratorToStream;
import static sfvfs.utils.StringUtils.generateEnLetters;

/**
 * @author alexey.kutuzov
 */
class DirectoryTest {

    private Random r;
    private DataBlocks dataBlocks;
    private File tempFile;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = File.createTempFile("sfvsf", ".dat");
        tempFile.deleteOnExit();
        dataBlocks = new DataBlocks(tempFile, 64, 2, "rw", 10 * 1024, 100);
        r = new Random(0);
    }

    @Test
    void createSimpleDirectoryAddAndListEntity() throws IOException {
        final Directory directory = createDirectory();
        directory.addEntity("test", 1234, new Flags.DirectoryListEntityFlags());

        System.out.println(directory);

        final List<DirectoryEntity> entities = iterToList(directory.listEntities());
        assertEquals(1, directory.size());
        assertEquals(1, entities.size());
        assertNotNull(directory.find("test"));

        assertEquals("test", entities.get(0).getName());
    }

    @Test
    void simpleDelete() throws IOException {
        final Directory directory = createDirectory();
        directory.addEntity("test", 1234, new Flags.DirectoryListEntityFlags());

        directory.removeEntity("test1");
        assertEquals(1, directory.size());
        assertEquals(1, iterToList(directory.listEntities()).size());

        directory.removeEntity("test");

        assertEquals(0, directory.size());
        assertEquals(0, iterToList(directory.listEntities()).size());
    }

    @Test
    void flags() throws IOException {
        final Directory directory = createDirectory();
        final Flags.DirectoryListEntityFlags flags = new Flags.DirectoryListEntityFlags();
        flags.setDirectory(true);
        directory.addEntity("test", 1234, flags);
        assertTrue(directory.find("test").isDirectory());
    }

    @Test
    void addMultipleEntitiesFixedLen() throws IOException {
        for (int i = 1; i < 30; i++) {

            final Directory directory = createDirectory();
            final Map<String, Integer> existing = new HashMap<>();

            for (int j = 0; j < 100; j++) {
                r.setSeed(i * j);

                final String name = generateEnLetters(r, i);
                if (existing.containsKey(name)) {
                    continue;
                }
                existing.put(name, i * j + 1);
                directory.addEntity(name, i * j + 1, new Flags.DirectoryListEntityFlags());

                validateDirectoryListing(directory, existing);
            }

            System.out.println(directory);
        }
    }

    @Test
    void addMultipleEntitiesRandomLen() throws IOException {
        final Directory directory = createDirectory();
        final Map<String, Integer> existing = new HashMap<>();

        for (int j = 1; j < 1000; j++) {
            r.setSeed(j);

            final String name = generateEnLetters(r, r.nextInt(25) + 1);
            if (existing.containsKey(name)) {
                continue;
            }
            existing.put(name, j);
            directory.addEntity(name, j, new Flags.DirectoryListEntityFlags());

            validateDirectoryListing(directory, existing);
        }

        System.out.println(directory);
    }

    @Test
    void addAndDeleteMultipleEntitiesNoIndex() throws IOException {
        checkAddDeleteMultipleEntities(createDirectory());
    }

    @Test
    void addAndDeleteMultipleEntitiesIndexed() throws IOException {
        checkAddDeleteMultipleEntities(createDirectory(10));
    }

    @Test
    void addALotEntitiesToIndexedDir() throws IOException {
        final DataBlocks dataBlocks = new DataBlocks(tempFile, 1024, 2, "rw", 10 * 1024, 100);

        final DataBlocks.Block block = dataBlocks.allocateBlock();
        final Directory directory = new Directory(dataBlocks, block.getAddress(), 30, 10);
        directory.create();

        final int iters = 10_000;

        for (int j = 1; j < iters; j++) {
            directory.addEntity("" + j, j, new Flags.DirectoryListEntityFlags());
        }

        System.out.println(directory);

        for (int j = 1; j < iters; j++) {
            assertEquals(j, directory.find("" + j).getAddress());
        }

        for (int j = 1; j < iters; j++) {
            directory.removeEntity("" + j);
        }

        assertEquals(0, directory.size());

        for (int j = 1; j < iters; j++) {
            assertNull(directory.find("" + j));
        }
    }

    @Test
    void deleteDirectory() throws IOException {
        final Directory directory = createDirectory();

        for (int j = 1; j < 1000; j++) {
            directory.addEntity("i" + j, j, new Flags.DirectoryListEntityFlags());
        }

        assertEquals(202, dataBlocks.getTotalBlocks() - dataBlocks.getFreeBlocks());

        for (int j = 1; j < 1000; j++) {
            directory.removeEntity("i" + j);
        }

        directory.delete();

        assertEquals(4, dataBlocks.getTotalBlocks() - dataBlocks.getFreeBlocks());
    }

    private void checkAddDeleteMultipleEntities(final Directory directory) throws IOException {
        final Map<String, Integer> existing = new HashMap<>();
        final List<String> names = new ArrayList<>();

        for (int i = 1; i <= 3; i++) {
            for (int j = 1; j <= 10; j++) {
                for (int k = 0; k < 100; k++) {
                    r.setSeed(i * j * k);

                    if (r.nextInt(10) + 1 > j ) {
                        final String name = generateEnLetters(r, r.nextInt(25) + 1);
                        final int address = i * j * k + 1;

                        if (existing.containsKey(name)) {
                            continue;
                        }

                        existing.put(name, address);
                        names.add(name);
                        directory.addEntity(name, address, new Flags.DirectoryListEntityFlags());
                    } else {
                        if (names.size() > 0) {
                            final String toRemove = names.remove(r.nextInt(names.size()));
                            existing.remove(toRemove);
                            directory.removeEntity(toRemove);
                        }
                    }

                    validateDirectoryListing(directory, existing);
                }

                System.out.println(directory);
            }
        }
    }

    private void validateDirectoryListing(final Directory directory, final Map<String, Integer> existing) throws IOException {
        assertEquals(existing.size(), directory.size());

        final HashMap<String, Integer> validating = new HashMap<>(existing);
        final Iterator<DirectoryEntity> entityIterator = directory.listEntities();

        while (entityIterator.hasNext()) {
            final DirectoryEntity entity = entityIterator.next();
            final Integer existingAddress = validating.remove(entity.getName());
            if (existingAddress != entity.getAddress()) {
                System.out.println(directory);
                fail(entity.getName() + " has wrong address");
            }
        }

        if (!validating.isEmpty()) {
            System.out.println(directory);
            fail("not found: " + validating);
        }
    }

    private <T> List<T> iterToList(final Iterator<T> iter) {
        return iteratorToStream(iter).collect(Collectors.toList());
    }

    private Directory createDirectory() throws IOException {
        return createDirectory(Integer.MAX_VALUE);
    }

    private Directory createDirectory(final int directoryMinSizeToBecomeIndexed) throws IOException {
        final DataBlocks.Block block = dataBlocks.allocateBlock();
        final Directory directory = new Directory(dataBlocks, block.getAddress(), 30, directoryMinSizeToBecomeIndexed);
        directory.create();
        return directory;
    }

}