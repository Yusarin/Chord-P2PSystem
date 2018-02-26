package Process;


import java.util.PriorityQueue;

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
        assert c1.compareTo(c2) == 0;
        assert c2.compareTo(c3) < 0;
        assert c3.compareTo(c2) > 0;
        assert c4.compareTo(c3) > 0;
        assert c3.lessByOne(c4);//less by one
        assert !c2.lessByOne(c4);//not less by one
        assert c2.compareTo(c5) < 0;
        PriorityQueue<VectorClock> p = new PriorityQueue<>();
        p.add(c1);
        p.add(c2);
        p.add(c3);
        p.add(c4);
        p.add(c5);
        for (VectorClock c :
                p) {
            System.out.println(c);
        }
    }
}
