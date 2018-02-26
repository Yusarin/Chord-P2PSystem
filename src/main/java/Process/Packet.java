package Process;

public class Packet implements Comparable {
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

    public byte[] serialize() {
        return null;
        //TODO: serialize the msg and clock
    }

    @Override
    public int compareTo(Object o) {
        return clock.compareTo(((Packet) o).clock);
    }
}
