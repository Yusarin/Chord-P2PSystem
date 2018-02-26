package Process;

public class VectorClock implements Comparable {
    private int length;
    private int[] clock;

    public VectorClock(int[] c) {
        length = c.length;
        clock = c;
    }

    public boolean lessByOne(VectorClock c) {
        boolean diffOne = false;
        for (int i = 0; i < length; ++i) {
            if (clock[i] + 1 == c.clock[i]) {
                if (!diffOne) {
                    diffOne = true;
                } else {
                    return false;
                }
            }
        }
        return true;
    }

    public void incAt(int pos) {
        clock[pos]++;
    }

    /**
     * @param c a VectorClock
     */
    public void update(VectorClock c) {
        clock = c.clock;
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof VectorClock) {
            for (int i = 0; i < length; i++) {
                if (clock[i] != ((VectorClock) o).clock[i]) {
                    return false;
                }
            }
        }
        return true;
    }

    @Override
    public int compareTo(Object o) {
        boolean less = false;
        if (o == null) {
            return 0;
        }// unicast
        if (o instanceof VectorClock) {
            for (int i = 0; i < length; ++i) {
                if (clock[i] < ((VectorClock) o).clock[i]) {
                    less = true;
                } else if (clock[i] > ((VectorClock) o).clock[i]) {
                    return 1;
                }
            }
        }
        if (less) {
            return -1;
        } else {
            return 0;
        }
    }
}
