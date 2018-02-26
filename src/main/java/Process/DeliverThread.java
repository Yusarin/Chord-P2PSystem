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
                VectorClock incoming = ((Packet) queue.peek()).getClock();
                if (clock == null || clock.lessByOne(incoming)) {
                    Packet p = (Packet) queue.take();
                    System.out.println("Received: " + p.getMsg());
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
