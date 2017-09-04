package sfvfs.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;

/**
 * @author alexey.kutuzov
 */
public class IOUtils {

    public static void copy(final InputStream is, final OutputStream os, final int bufferSize) throws IOException {
        final byte[] buffer = new byte[bufferSize];
        int len;
        while ((len = is.read(buffer)) != -1) {
            os.write(buffer, 0, len);
        }
    }

    public static boolean isEqual(final InputStream i1, final InputStream i2, final int bufferSize) throws IOException {
        final ReadableByteChannel ch1 = Channels.newChannel(i1);
        final ReadableByteChannel ch2 = Channels.newChannel(i2);

        final ByteBuffer buf1 = ByteBuffer.allocateDirect(bufferSize);
        final ByteBuffer buf2 = ByteBuffer.allocateDirect(bufferSize);

        while (true) {
            final int n1 = ch1.read(buf1);
            final int n2 = ch2.read(buf2);

            if (n1 == -1 || n2 == -1) {
                return n1 == n2;
            }

            buf1.flip();
            buf2.flip();

            for (int i = 0; i < Math.min(n1, n2); i++) {
                if (buf1.get() != buf2.get()) {
                    return false;
                }
            }

            buf1.compact();
            buf2.compact();
        }
    }

}
