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

    /**
     * For unicast
     *
     * @param queue
     * @param c
     */
    public DeliverThread(BlockingQueue queue, VectorClock c) {
        this(queue, c, null, null, Logger.getLogger(DeliverThread.class.getName()));
    }

    /**
     * For causal multicast
     *
     * @param queue
     * @param c
     * @param lock
     * @param condition
     * @param LOGGER
     */
    public DeliverThread(BlockingQueue queue, VectorClock c, Lock lock, Condition condition, Logger LOGGER) {
        this.queue = queue;
        this.clock = c;
        this.lock = lock;
        this.condition = condition;
        this.LOGGER = LOGGER;
    }

    /**
     * update the clock
     * @param c incoming clock
     */
    private synchronized void updateClock(VectorClock c) {
        LOGGER.finest("updating clock");
        clock.update(c);
    }

    /**
     * Run the deliver thread.
     *
     * If the clock variable is null, then this thread is unicast_mode. It basically ignore the vector clock
     * and print message immediately.
     *
     * If the clock is not null, then this thread is in causal_mode. It doesn't deliver the packet immediately.
     * Each time this thread get signalled, it check the top packet. If the packet has expected packet (implemented by
     * asExpected method of class VecterClock), then it deliver and update the shared clock within the critical section..
     */
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
                        if (queue.peek() != null)
                            LOGGER.fine("top packet clock: " + ((Packet) queue.peek()).getClock());
                        condition.await();//block deliver thread if no expected packet arrives.
                    }
                    p = (Packet) queue.take();
                    updateClock(p.getClock());
                    lock.unlock();
                    LOGGER.info("Received message: (" + p.getMsg() + ") from process: " + p.getSenderId() + " at system time: " + new Date());
                    LOGGER.info("Current VectorClock: " + clock);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
