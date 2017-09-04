package sfvfs.internal;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.*;
import static org.junit.Assert.assertArrayEquals;

/**
 * @author alexey.kutuzov
 */
public class DataBlocksTest {

    private DataBlocks dataBlocks;
    private File tempFile;

    @Before
    public void setUp() throws IOException {
        tempFile = File.createTempFile("sfvsf", ".dat");
        tempFile.deleteOnExit();
        dataBlocks = new DataBlocks(tempFile, 64, 1, "rw", 10 * 1024, 100);
    }

    @Test
    public void simpleAllocateAndDeallocateBlock() throws IOException {
        final DataBlocks.Block block = dataBlocks.allocateBlock();
        assertNotNull(block);

        assertEquals(64, dataBlocks.getTotalBlocks());
        assertEquals(62, dataBlocks.getFreeBlocks());

        dataBlocks.deallocateBlock(block.getAddress());
        assertEquals(63, dataBlocks.getFreeBlocks());
    }

    @Test
    public void allocateAndDeallocateMultipleBlocks() throws IOException {
        final List<DataBlocks.Block> blocks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            blocks.add(dataBlocks.allocateBlock());
        }

        assertEquals(128, dataBlocks.getTotalBlocks());
        assertEquals(26, dataBlocks.getFreeBlocks());

        for (final DataBlocks.Block block : blocks) {
            dataBlocks.deallocateBlock(block.getAddress());
        }

        assertEquals(128, dataBlocks.getTotalBlocks());
        assertEquals(126, dataBlocks.getFreeBlocks());

        for (int i = 0; i < 128; i++) {
            blocks.add(dataBlocks.allocateBlock());
        }

        assertEquals(192, dataBlocks.getTotalBlocks());
        assertEquals(61, dataBlocks.getFreeBlocks());
    }

    @Test
    public void reinitializeBlocks() throws IOException {
        for (int i = 0; i < 100; i++) {
            dataBlocks.allocateBlock();
        }

        assertEquals(128, dataBlocks.getTotalBlocks());
        assertEquals(26, dataBlocks.getFreeBlocks());

        final DataBlocks anotherBlocks = new DataBlocks(tempFile, 64, 1, "rw", 10 * 1024, 100);

        assertEquals(128, anotherBlocks.getTotalBlocks());
        assertEquals(26, anotherBlocks.getFreeBlocks());
    }

    @Test
    public void writeAndReadData() throws IOException {
        final DataBlocks.Block block = dataBlocks.allocateBlock();
        block.write(new byte[]{1, 2, 3, 4});

        final DataBlocks.Block anotherBlock = dataBlocks.getBlock(block.getAddress());
        final byte[] read = anotherBlock.read();

        assertEquals(64, read.length);
        assertArrayEquals(new byte[] {1, 2, 3, 4}, Arrays.copyOfRange(read, 0, 4));
    }

    @Test
    public void compact() throws IOException {
        final Random r = new Random();
        int total = 0;
        final List<Integer> allocatedBlocks = new ArrayList<>();

        for (int i = 1; i < 10; i++) {
            r.setSeed(i);

            for (int j = 0; j < 1000; j++) {
                final DataBlocks.Block block = dataBlocks.allocateBlock();
                allocatedBlocks.add(block.getAddress());

                total += i * j;
                block.writeInt(0, i * j);
            }

            for (int j = 0; j < 1000; j++) {
                if (r.nextBoolean()) {
                    if (allocatedBlocks.size() > 0) {
                        final Integer blockAddress = allocatedBlocks.remove(r.nextInt(allocatedBlocks.size()));
                        total -= dataBlocks.getBlock(blockAddress).readInt(0);
                        dataBlocks.deallocateBlock(blockAddress);
                    }
                }
            }

            assertTrue(dataBlocks.getFreeBlocks() > 64);

            dataBlocks.compact();

            assertTrue(dataBlocks.getFreeBlocks() < 64);

            int currentTotal = 0;
            for (final Integer blockAddress : allocatedBlocks) {
                final DataBlocks.Block block = dataBlocks.getBlock(blockAddress);
                currentTotal += block.readInt(0);
            }

            assertEquals(total, currentTotal);
        }
    }

}