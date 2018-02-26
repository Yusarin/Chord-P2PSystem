package Process;

import java.io.Serializable;

public class Packet implements Comparable, Serializable {
    private final String msg;
    private final VectorClock clock;

    public Packet(String msg) {
        this.msg = msg;
        this.clock = null;
    }

    public Packet(String msg, VectorClock clock) {
        this.msg = msg;
        this.clock = clock;
    }

    public String getMsg() {
        return msg;
    }


    public VectorClock getClock() {
        return clock;
    }

    @Override
    public int compareTo(Object o) {
        return clock.compareTo(((Packet) o).clock);
    }

}
