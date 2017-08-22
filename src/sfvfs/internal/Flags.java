package sfvfs.internal;

/**
 * @author alexey.kutuzov
 */
class Flags {

    static class BlockGroupFlags {

        private static final byte TAKEN = 0x1;

        private byte flags;

        BlockGroupFlags(final byte flags) {
            this.flags = flags;
        }

        boolean isTaken() {
            return (flags & TAKEN) == TAKEN;
        }

        void setTaken(final boolean taken) {
            if (taken) {
                flags |= TAKEN;
            } else {
                flags &= ~TAKEN;
            }
        }

        byte value() {
            return flags;
        }
    }

    static class InodeFlags {

        private static final int NEED_EMPTY_BLOCK = 0x1;
        private static final int LOCKED = 0x2;

        private int flags;

        InodeFlags(final int flags) {
            this.flags = flags;
        }

        boolean isNeedEmptyBlock() {
            return (flags & NEED_EMPTY_BLOCK) == NEED_EMPTY_BLOCK;
        }

        void setNeedEmptyBlock(final boolean need) {
            if (need) {
                flags |= NEED_EMPTY_BLOCK;
            } else {
                flags &= ~NEED_EMPTY_BLOCK;
            }
        }

        boolean isLocked() {
            return (flags & LOCKED) == LOCKED;
        }

        void setLocked(final boolean locked) {
            if (locked) {
                flags |= LOCKED;
            } else {
                flags &= ~LOCKED;
            }
        }

        int value() {
            return flags;
        }

        @Override
        public String toString() {
            return Integer.toBinaryString(flags);
        }
    }

}
