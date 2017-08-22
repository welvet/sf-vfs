package sfvfs.utils;

import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author alexey.kutuzov
 */
public class Streams {
    public static <T> Stream<T> iteratorToStream(final Iterator<T> iterator) {
        final Iterable<T> iterable = () -> iterator;
        return StreamSupport.stream(iterable.spliterator(), false);
    }
}
