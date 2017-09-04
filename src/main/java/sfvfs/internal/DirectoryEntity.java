package sfvfs.internal;

/**
 * @author alexey.kutuzov
 */
public interface DirectoryEntity {
    String getName();

    int getAddress();

    boolean isDirectory();

    int getParentDirectoryAddress();

    Flags.DirectoryListEntityFlags getFlags();
}
