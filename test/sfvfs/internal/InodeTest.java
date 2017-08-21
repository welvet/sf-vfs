package sfvfs.internal;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * @author alexey.kutuzov
 */
class InodeTest {

    private Random r;
    private DataBlocks dataBlocks;

    @BeforeEach
    void setUp() throws IOException {
        final File tempFile = File.createTempFile("sfvsf", ".dat");
        tempFile.deleteOnExit();
        dataBlocks = new DataBlocks(tempFile, 64, 2);
        r = new Random(0);
    }

    @Test
    void simpleReadAndWriteInode() throws IOException {
        final Inode inode = newInode();
        assertEquals(0, inode.getSize());

        append(inode, "abcd");

        assertEquals(4, inode.getSize());
        assertEquals("abcd", read(inode));
    }

    @Test
    void longTextWriteAndRead() throws IOException {
        for (int i = 0; i < 1_000; i ++) {
            r.setSeed(i);

            System.out.println("Checking " + i);

            final String generatedText = generateText(i);

            final Inode inode = newInode();
            assertEquals(0, inode.getSize());

            append(inode, generatedText);
            printInodes(inode);

            assertEquals(generatedText.getBytes().length, inode.getSize());
            assertEquals(generatedText, read(inode));
        }
    }

    @Test
    void longTextWriteAndReadInodeClear() throws IOException {
        final Inode inode = newInode();
        for (int i = 0; i < 1_000; i ++) {
            r.setSeed(i);
            System.out.println("Checking " + i);

            inode.clear();
            printInodes(inode);
            final int takenBlocks = dataBlocks.debugGetTotalBlocks() - dataBlocks.debugGetFreeBlocks();
            assertTrue(6 >= takenBlocks, takenBlocks + "");

            final String generatedText = generateText(i);

            assertEquals(0, inode.getSize());

            append(inode, generatedText);
            printInodes(inode);

            assertEquals(generatedText.getBytes().length, inode.getSize());
            assertEquals(generatedText, read(inode));
        }
    }

    @Test
    void longTextWriteAndReadInodeDelete() throws IOException {
        for (int i = 0; i < 1_000; i ++) {
            r.setSeed(i);
            System.out.println("Checking " + i);

            final Inode inode = newInode();
            final int takenBlocks = dataBlocks.debugGetTotalBlocks() - dataBlocks.debugGetFreeBlocks();
            assertTrue(6 >= takenBlocks, takenBlocks + "");

            final String generatedText = generateText(i);

            assertEquals(0, inode.getSize());

            append(inode, generatedText);
            printInodes(inode);

            assertEquals(generatedText.getBytes().length, inode.getSize());
            assertEquals(generatedText, read(inode));

            inode.delete();
        }
    }

    @Test
    void appendBlockSizeText() throws IOException {
        final Inode inode = newInode();

        final StringBuilder completeText = new StringBuilder();

        for (int i = 0; i < 1_000; i ++) {
            r.setSeed(i);

            final String generatedText = "lkjasdlkjasdlkjasdlkjaslkjaslkja";
            completeText.append(generatedText);

            System.out.println("Checking " + i + " size: " + completeText.toString().getBytes().length);

            append(inode, generatedText);
            printInodes(inode);

            assertEquals(completeText.toString().getBytes().length, inode.getSize());
            assertEquals(completeText.toString(), read(inode));
        }
    }

    @Test
    void appendToExistingFile() throws IOException {
        final Inode inode = newInode();

        final StringBuilder completeText = new StringBuilder();

        for (int i = 0; i < 100; i ++) {
            r.setSeed(i);

            final String generatedText = generateText(i);
            completeText.append(generatedText);

            System.out.println("Checking " + i + " size: " + completeText.toString().getBytes().length);

            append(inode, generatedText);
            printInodes(inode);

            assertEquals(completeText.toString().getBytes().length, inode.getSize());
            assertEquals(completeText.toString(), read(inode));
        }
    }

    @Test
    void multipleFlush() throws IOException {
        final Inode inode = newInode();

        final StringBuilder completeText = new StringBuilder();

        for (int i = 0; i < 500; i ++) {
            r.setSeed(i);

            try (OutputStream os = inode.appendStream()) {
                final String generatedText = generateText(i);
                completeText.append(generatedText);

                os.write(generatedText.getBytes());
                os.flush();
            }
        }

        printInodes(inode);

        assertEquals(completeText.toString().getBytes().length, inode.getSize());
        assertEquals(completeText.toString(), read(inode));
    }

    private void append(final Inode inode, final String text) throws IOException {
        try (OutputStream os = inode.appendStream()) {
            os.write(text.getBytes());
        }
    }

    private String read(final Inode inode) throws IOException {
        final StringBuilder out = new StringBuilder();
        final char[] buffer = new char[1024];

        try (Reader in = new InputStreamReader(inode.readStream())) {
            while (true) {
                final int read = in.read(buffer, 0, buffer.length);
                if (read < 0) {
                    break;
                }
                out.append(buffer, 0, read);
            }
        }

        return out.toString();
    }

    private Inode newInode() throws IOException {
        final DataBlocks.Block block = dataBlocks.allocateBlock();
        block.clear();

        return new Inode(dataBlocks, block.getAddress());
    }

    private String generateText(final int len) {
        final StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {
            char c = (char) r.nextInt();
            while (!Character.isAlphabetic(c)) {
                c = (char) r.nextInt();
            }

            sb.append(c);
        }

        return sb.toString();
    }

    private void printInodes(final Inode inode) throws IOException {
        Inode in = inode;

        while (true) {
            System.out.println(in.toString());

            final int nextInodeAddress = in.debugGetNextInode();
            if (nextInodeAddress != 0) {
                in = new Inode(dataBlocks, nextInodeAddress);
            } else {
                break;
            }
        }
    }
}