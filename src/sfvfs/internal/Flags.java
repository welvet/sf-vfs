package sfvfs.internal;

/**
 * @author alexey.kutuzov
 */
public class Flags {

    public static class BlockGroupFlags {

        private static final byte TAKEN = 0x1;

        private byte flags;

        public BlockGroupFlags(final byte flags) {
            this.flags = flags;
        }

        public boolean isTaken() {
            return (flags & TAKEN) == TAKEN;
        }

        public void setTaken(final boolean taken) {
            if (taken) {
                flags |= TAKEN;
            } else {
                flags &= ~TAKEN;
            }
        }

        public byte value() {
            return flags;
        }
    }

    public static class InodeFlags {
        private int flags;

        private static final int NEED_EMPTY_BLOCK = 0x1;

        public InodeFlags(final int flags) {
            this.flags = flags;
        }

        public boolean needEmptyBlock() {
            return (flags & NEED_EMPTY_BLOCK) == NEED_EMPTY_BLOCK;
        }

        public void setNeedEmptyBlock(final boolean need) {
            if (need) {
                flags |= NEED_EMPTY_BLOCK;
            } else {
                flags &= ~NEED_EMPTY_BLOCK;
            }
        }

        public int value() {
            return flags;
        }

        @Override
        public String toString() {
            return Integer.toBinaryString(flags);
        }
    }

}
