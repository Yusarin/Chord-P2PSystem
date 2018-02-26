package Process;

public class Packet implements Comparable {
    private String msg;
    private VectorClock clock;

    public Packet(String msg) {
        this.msg = msg;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public VectorClock getClock() {
        return clock;
    }

    public void setClock(VectorClock clock) {
        this.clock = clock;
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
