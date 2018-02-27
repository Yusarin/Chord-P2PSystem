package Process;


import java.io.Serializable;
import java.util.PriorityQueue;

public class VectorClock implements Comparable, Serializable {
    private int length;
    private int[] clock;

    public VectorClock(int[] c) {
        length = c.length;
        clock = c;
    }

    public boolean asExpected(VectorClock c) {
        boolean diffOne = false;
        for (int i = 0; i < length; ++i) {
            if (clock[i] + 1 == c.clock[i]) {
                if (!diffOne) {// If only one position is less by 1, the incoming clock is as expected.
                    diffOne = true;
                } else {//If more than one positions are less by 1, the incoming clock is not as expected.
                    return false;
                }
            } else if (clock[i] + 1 < c.clock[i]) {// If at position i, current clock is less by more than 1, the incoming clock is not as expected.
                return false;
            }
        }
        return diffOne;
    }

    public void incAt(int pos) {
        clock[pos]++;
    }

    /**
     * Pick the larger value between two clock
     * @param c a VectorClock
     */
    public void update(VectorClock c) {
        for (int i = 0; i < length; ++i) {
            clock[i] = clock[i] > c.clock[i] ? clock[i] : c.clock[i];
        }
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

    @Override
    public String toString() {
        StringBuilder s = new StringBuilder();
        s.append('(');
        for (int i : clock) {
            s.append(i);
            s.append(';');
        }
        s.append(')');
        return s.toString();
    }

    /**
     * Simple test for VectorClock
     */
    public static void main(String[] args) {
        VectorClock c1 = new VectorClock(new int[]{1, 1, 1});
        VectorClock c2 = new VectorClock(new int[]{1, 1, 1});
        VectorClock c3 = new VectorClock(new int[]{1, 2, 1});
        VectorClock c4 = new VectorClock(new int[]{1, 2, 2});
        VectorClock c5 = new VectorClock(new int[]{1, 3, 0});
        VectorClock c6 = new VectorClock(new int[]{1, 3, 1});
        assert c1.compareTo(c2) == 0;
        assert c2.compareTo(c3) < 0;
        assert c3.compareTo(c2) > 0;
        assert c4.compareTo(c3) > 0;
        assert c2.compareTo(c5) < 0;
        assert c3.asExpected(c4);//less by one
        assert c5.asExpected(c6);//less by one
        assert !c2.asExpected(c4);//not less by one
        assert !c1.asExpected(c5);//less by two
        PriorityQueue<VectorClock> p = new PriorityQueue<>();
        p.add(c1);
        p.add(c2);
        p.add(c3);
        p.add(c4);
        p.add(c5);
        p.add(c6);
        for (VectorClock c :
                p) {
            System.out.println(c);
        }
    }
}
