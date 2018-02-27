package Process;

import java.io.Serializable;

public class Packet implements Comparable, Serializable {
    private final String msg;
    private final VectorClock clock;
    private final int senderId;

    public Packet(int senderId, String msg) {
        this(senderId, msg, null);
    }

    public Packet(int senderId, String msg, VectorClock clock) {
        this.senderId = senderId;
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

    public int getSenderId() {
        return senderId;
    }
}
