package sfvfs.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author alexey.kutuzov
 */
class DataBlocksTest {

    private DataBlocks dataBlocks;
    private File tempFile;

    @BeforeEach
    void setUp() throws IOException {
        tempFile = File.createTempFile("sfvsf", ".dat");
        tempFile.deleteOnExit();
        dataBlocks = new DataBlocks(tempFile, 64, 1);
    }

    @Test
    void simpleAllocateAndDeallocateBlock() throws IOException {
        final DataBlocks.Block block = dataBlocks.allocateBlock();
        assertNotNull(block);

        assertEquals(64, dataBlocks.debugGetTotalBlocks());
        assertEquals(62, dataBlocks.debugGetFreeBlocks());

        dataBlocks.deallocateBlock(block.getAddress());
        assertEquals(63, dataBlocks.debugGetFreeBlocks());
    }

    @Test
    void allocateAndDeallocateMultipleBlocks() throws IOException {
        final List<DataBlocks.Block> blocks = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            blocks.add(dataBlocks.allocateBlock());
        }

        assertEquals(128, dataBlocks.debugGetTotalBlocks());
        assertEquals(26, dataBlocks.debugGetFreeBlocks());

        for (final DataBlocks.Block block : blocks) {
            dataBlocks.deallocateBlock(block.getAddress());
        }

        assertEquals(128, dataBlocks.debugGetTotalBlocks());
        assertEquals(126, dataBlocks.debugGetFreeBlocks());

        for (int i = 0; i < 128; i++) {
            blocks.add(dataBlocks.allocateBlock());
        }

        assertEquals(192, dataBlocks.debugGetTotalBlocks());
        assertEquals(61, dataBlocks.debugGetFreeBlocks());
    }

    @Test
    void reinitializeBlocks() throws IOException {
        for (int i = 0; i < 100; i++) {
            dataBlocks.allocateBlock();
        }

        assertEquals(128, dataBlocks.debugGetTotalBlocks());
        assertEquals(26, dataBlocks.debugGetFreeBlocks());

        final DataBlocks anotherBlocks = new DataBlocks(tempFile, 64, 1);

        assertEquals(128, anotherBlocks.debugGetTotalBlocks());
        assertEquals(26, anotherBlocks.debugGetFreeBlocks());
    }

    @Test
    void writeAndReadData() throws IOException {
        final DataBlocks.Block block = dataBlocks.allocateBlock();
        block.write(new byte[]{1, 2, 3, 4});

        final DataBlocks.Block anotherBlock = dataBlocks.getBlock(block.getAddress());
        final byte[] read = anotherBlock.read();

        assertEquals(64, read.length);
        assertArrayEquals(new byte[] {1, 2, 3, 4}, Arrays.copyOfRange(read, 0, 4));
    }

}