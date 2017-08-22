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

    public static void checkArgument(final boolean argsCorrect, final String template, final Object... args){
        if (!argsCorrect) {
            throw new IllegalArgumentException(String.format(template, args));
        }
    }

    public static void checkState(final boolean stateCorrect, final String template, final Object... args ) {
        if (!stateCorrect) {
            throw new IllegalStateException(String.format(template, args));
        }
    }

}
