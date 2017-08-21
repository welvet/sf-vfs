package sfvfs.internal;

import java.util.BitSet;

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

}
