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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static sfvfs.utils.Streams.iteratorToStream;
import static sfvfs.utils.StringUtils.generateEnLetters;

/**
 * @author alexey.kutuzov
 */
class DirectoryTest {

    private Random r;
    private DataBlocks dataBlocks;

    @BeforeEach
    void setUp() throws IOException {
        final File tempFile = File.createTempFile("sfvsf", ".dat");
        tempFile.deleteOnExit();
        dataBlocks = new DataBlocks(tempFile, 64, 2, "rw");
        r = new Random(0);
    }

    @Test
    void createSimpleDirectoryAddAndListEntity() throws IOException {
        final Directory directory = createDirectory();
        directory.addEntity("test", 1234);

        System.out.println(directory);

        final List<Directory.Entity> entities = iterToList(directory.listEntities());
        assertEquals(1, directory.size());
        assertEquals(1, entities.size());
        assertTrue(directory.exists("test"));

        assertEquals("test", entities.get(0).getName());
    }

    @Test
    void simpleDelete() throws IOException {
        final Directory directory = createDirectory();
        directory.addEntity("test", 1234);

        directory.removeEntity("test1");
        assertEquals(1, directory.size());
        assertEquals(1, iterToList(directory.listEntities()).size());

        directory.removeEntity("test");

        assertEquals(0, directory.size());
        assertEquals(0, iterToList(directory.listEntities()).size());
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
                directory.addEntity(name, i * j + 1);

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
            directory.addEntity(name, j);

            validateDirectoryListing(directory, existing);
        }

        System.out.println(directory);
    }

    @Test
    void addAndDeleteMultipleEntities() throws IOException {
        final Directory directory = createDirectory();
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
                        directory.addEntity(name, address);
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

    @Test
    void deleteDirectory() throws IOException {
        final Directory directory = createDirectory();

        for (int j = 1; j < 1000; j++) {
            directory.addEntity("i" + j, j);
        }

        assertEquals(169, dataBlocks.debugGetTotalBlocks() - dataBlocks.debugGetFreeBlocks());

        for (int j = 1; j < 1000; j++) {
            directory.removeEntity("i" + j);
        }

        directory.delete();

        assertEquals(4, dataBlocks.debugGetTotalBlocks() - dataBlocks.debugGetFreeBlocks());
    }

    private void validateDirectoryListing(final Directory directory, final Map<String, Integer> existing) throws IOException {
        assertEquals(existing.size(), directory.size());

        final HashMap<String, Integer> validating = new HashMap<>(existing);
        final Iterator<Directory.Entity> entityIterator = directory.listEntities();

        while (entityIterator.hasNext()) {
            final Directory.Entity entity = entityIterator.next();
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
        final DataBlocks.Block block = dataBlocks.allocateBlock();
        final Directory directory = new Directory(dataBlocks, block.getAddress(), 30);
        directory.create();
        return directory;
    }

}