package Process;

import java.util.concurrent.BlockingQueue;

public class DeliverThread implements Runnable {
    private BlockingQueue queue;
    private final VectorClock clock;

    public DeliverThread(BlockingQueue queue, VectorClock c) {
        this.queue = queue;
        this.clock = c;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (clock == null || clock.lessByOne(((Packet) queue.peek()).getClock())) {
                    Packet p = (Packet) queue.take();
                    System.out.println("Received by deliver thread: " + p.getMsg());
                    if (clock != null) // if clock is null, then it is unicast
                        clock.update(p.getClock());
                }
                //TODO: run the deliver thread
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
