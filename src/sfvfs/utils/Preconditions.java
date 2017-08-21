package sfvfs.utils;

/**
 * @author alexey.kutuzov
 */
public class Preconditions {

    public static void checkNotNull(final Object val, final String fieldName) {
        if (val == null) {
            throw new NullPointerException(fieldName + " is null");
        }
    }

    public static void checkArgument(final boolean argsCorrect, final String errors) {
        if (!argsCorrect) {
            throw new IllegalArgumentException(errors);
        }
    }

    public static void checkState(final boolean stateCorrect, final String errors) {
        if (!stateCorrect) {
            throw new IllegalStateException(errors);
        }
    }

}
