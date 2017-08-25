package sfvfs.utils;

import java.util.Random;

/**
 * @author alexey.kutuzov
 */
public class StringUtils {

    public static String generateText(final Random r, final int len) {
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

    public static String generateEnLetters(final Random r, final int len) {
        final StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {
            sb.append((char) (r.nextInt(26) + 'a'));
        }

        return sb.toString();
    }

}
