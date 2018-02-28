package Process;

import java.io.Serializable;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

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

    public static void main(String[] args) {
        Packet p1 = new Packet(1, "", new VectorClock(new int[]{1, 1, 1}));
        Packet p2 = new Packet(1, "", new VectorClock(new int[]{1, 1, 2}));
        assert p1.compareTo(p2) < 0;
        assert p2.compareTo(p1) > 0;
        BlockingQueue<Packet> q = new PriorityBlockingQueue<>();
        q.add(p2);
        q.add(p1);
        for (Packet i : q) {
            System.out.println(i.getClock());
        }
    }
}
