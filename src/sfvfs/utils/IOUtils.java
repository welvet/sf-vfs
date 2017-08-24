package sfvfs.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

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

}
