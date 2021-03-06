package sfvfs.internal;

import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.util.Random;

import static org.junit.Assert.*;
import static sfvfs.utils.StringUtils.generateText;

/**
 * @author alexey.kutuzov
 */
public class InodeTest {

    private Random r;
    private DataBlocks dataBlocks;

    @Before
    public void setUp() throws IOException {
        final File tempFile = File.createTempFile("sfvsf", ".dat");
        tempFile.deleteOnExit();
        dataBlocks = new DataBlocks(tempFile, 64, 2, "rw", 100 * 1024, 100);
        r = new Random(0);
    }

    @Test
    public void simpleReadAndWriteInode() throws IOException {
        final Inode inode = newInode();
        assertEquals(0, inode.getSize());

        append(inode, "abcd");

        assertEquals(4, inode.getSize());
        assertEquals("abcd", read(inode));
    }

    @Test
    public void longTextWriteAndRead() throws IOException {
        for (int i = 0; i < 1_000; i ++) {
            r.setSeed(i);

            System.out.println("Checking " + i);

            final String generatedText = generateText(r, i);

            final Inode inode = newInode();
            assertEquals(0, inode.getSize());

            append(inode, generatedText);
            printInodes(inode);

            assertEquals(generatedText.getBytes().length, inode.getSize());
            assertEquals(generatedText, read(inode));
        }
    }

    @Test
    public void longTextWriteAndReadInodeClear() throws IOException {
        final Inode inode = newInode();
        for (int i = 0; i < 1_000; i ++) {
            r.setSeed(i);
            System.out.println("Checking " + i);

            inode.clear();
            printInodes(inode);
            final int takenBlocks = dataBlocks.getTotalBlocks() - dataBlocks.getFreeBlocks();
            assertTrue(takenBlocks + "", 6 >= takenBlocks);

            final String generatedText = generateText(r, i);

            assertEquals(0, inode.getSize());

            append(inode, generatedText);
            printInodes(inode);

            assertEquals(generatedText.getBytes().length, inode.getSize());
            assertEquals(generatedText, read(inode));
        }

        dataBlocks.debugPrintBlockUsage();
    }

    @Test
    public void longTextWriteAndReadInodeDelete() throws IOException {
        for (int i = 0; i < 1_000; i ++) {
            r.setSeed(i);
            System.out.println("Checking " + i);

            final Inode inode = newInode();
            final int takenBlocks = dataBlocks.getTotalBlocks() - dataBlocks.getFreeBlocks();
            assertTrue(takenBlocks + "", 6 >= takenBlocks);

            final String generatedText = generateText(r, i);

            assertEquals(0, inode.getSize());

            append(inode, generatedText);
            printInodes(inode);

            assertEquals(generatedText.getBytes().length, inode.getSize());
            assertEquals(generatedText, read(inode));

            inode.delete();
        }

        dataBlocks.debugPrintBlockUsage();
    }

    @Test
    public void appendBlockSizeText() throws IOException {
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
    public void appendToExistingFile() throws IOException {
        final Inode inode = newInode();

        final StringBuilder completeText = new StringBuilder();

        for (int i = 0; i < 100; i ++) {
            r.setSeed(i);

            final String generatedText = generateText(r, i);
            completeText.append(generatedText);

            System.out.println("Checking " + i + " size: " + completeText.toString().getBytes().length);

            append(inode, generatedText);
            printInodes(inode);

            assertEquals(completeText.toString().getBytes().length, inode.getSize());
            assertEquals(completeText.toString(), read(inode));
        }
    }

    @Test
    public void appendBinary() throws IOException {
        final Inode inode = newInode();

        int sum = 0;
        try (OutputStream os = inode.appendStream()) {
            for (int i = 0; i < 1_000; i++) {
                r.setSeed(i);

                final byte[] bytes = new byte[1024];
                r.nextBytes(bytes);
                for (final byte aByte : bytes) {
                    sum += aByte;
                }
                os.write(bytes);
            }
        }

        printInodes(inode);

        int anotherSum = 0;
        try (final InputStream inputStream = inode.readStream()) {
            while (true) {
                final int v = inputStream.read();
                if (v == -1) {
                    break;
                }

                anotherSum += (byte) v;
            }
        }

        assertEquals(anotherSum, sum);
    }

    @Test
    public void multipleFlush() throws IOException {
        final Inode inode = newInode();

        final StringBuilder completeText = new StringBuilder();

        for (int i = 0; i < 500; i ++) {
            r.setSeed(i);

            try (OutputStream os = inode.appendStream()) {
                final String generatedText = generateText(r, i);
                completeText.append(generatedText);

                os.write(generatedText.getBytes());
                os.flush();
            }
        }

        printInodes(inode);

        assertEquals(completeText.toString().getBytes().length, inode.getSize());
        assertEquals(completeText.toString(), read(inode));
    }

    @Test
    public void creatingMultipleStreamsWithoutClosingFails() throws IOException {
        final Inode inode = newInode();
        final OutputStream outputStream = inode.appendStream();
        try {
            inode.readStream();
            fail("illegal state expected");
        } catch (final IllegalStateException ignored) {

        }

        outputStream.close();
        inode.readStream();
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

    private void printInodes(final Inode inode) throws IOException {
        Inode in = inode;

        while (true) {
            System.out.println(in.toString());

            final int nextInodeAddress = in.debugGetNextInodeAddress();
            if (nextInodeAddress != 0) {
                in = new Inode(dataBlocks, nextInodeAddress);
            } else {
                break;
            }
        }
    }

}