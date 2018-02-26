package Process;

import java.util.concurrent.BlockingQueue;

public class DeliverThread implements Runnable {
    private BlockingQueue queue;
    private final VectorClock clock;
    private final Object lock;

    public DeliverThread(BlockingQueue queue, VectorClock c) {
        this(queue, c, null);
    }

    public DeliverThread(BlockingQueue queue, VectorClock c, Object lock) {
        this.queue = queue;
        this.clock = c;
        this.lock = lock;
    }

    private synchronized void updateClock(VectorClock c) {
        clock.update(c);
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (clock == null) { // if clock is null, then it is unicast
                    Packet p = (Packet) queue.take();
                    System.out.println("Received by deliver thread: " + p.getMsg());
                } else {
                    synchronized (lock) {
                        if (clock.lessByOne(((Packet) queue.peek()).getClock())) {
                            Packet p = (Packet) queue.take();
                            System.out.println("Received by deliver thread: " + p.getMsg());
                            updateClock(p.getClock());
                            System.out.println("CurrentClock" + clock);
                        }
                    }
                }
                //TODO: run the deliver thread
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
