package Process;

import java.util.Date;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

public class DeliverThread implements Runnable {
    private BlockingQueue queue;
    private final VectorClock clock;
    private final Lock lock;
    private final Condition condition;
    private final Logger LOGGER;

    public DeliverThread(BlockingQueue queue, VectorClock c) {
        this(queue, c, null, null, Logger.getLogger(DeliverThread.class.getName()));
    }

    public DeliverThread(BlockingQueue queue, VectorClock c, Lock lock, Condition condition, Logger LOGGER) {
        this.queue = queue;
        this.clock = c;
        this.lock = lock;
        this.condition = condition;
        this.LOGGER = LOGGER;
    }

    private synchronized void updateClock(VectorClock c) {
        LOGGER.finest("updating clock");
        clock.update(c);
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (clock == null) { // if clock is null, then it is unicast
                    Packet p = (Packet) queue.take();
                    LOGGER.info("Received message: (" + p.getMsg() + ") from process: " + p.getSenderId() + " at system time: " + new Date());
                } else {
                    Packet p;
                    lock.lock();// shared memory
                    while (queue.peek() == null || !clock.asExpected(((Packet) queue.peek()).getClock())) {
                        condition.await();
                    }
                    p = (Packet) queue.take();
                    updateClock(p.getClock());
                    lock.unlock();
                    LOGGER.info("Received message: (" + p.getMsg() + ") from process: " + p.getSenderId() + " at system time: " + new Date());
                    LOGGER.info("Current VectorClock: " + clock);
                }
                //TODO: run the deliver thread
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
