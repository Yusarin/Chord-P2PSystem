package Process;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.logging.Logger;

import javax.swing.LookAndFeel;

public class DeliverThread implements Runnable {
    private BlockingQueue queue;
    private final VectorClock clock;
    private final Lock lock;
    private final Condition condition;
    private final Logger LOGGER;
    private final ArrayList<Packet> packetSet = new ArrayList<>();

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
                    LOGGER.info("Received message: (" + p.getMsg() + ") from process: " + p.getSenderId()
                            + " at system time: " + new Date());
                } else {
                    Packet incoming = (Packet) queue.take();
                    packetSet.add(incoming);
                    while (true) {
                        boolean foundExpected = false;
                        for (int i = 0; i < packetSet.size(); ++i) {//iterate all the buffered packet
                            Packet packet = packetSet.get(i);
                            if (clock.asExpected(packet.getClock())) {
                                LOGGER.info("Received message: (" + packet.getMsg() + ") from process: "
                                        + packet.getSenderId() + " at system time: " + new Date());
                                packetSet.remove(i);
                                foundExpected = true;
                                lock.lock();// shared memory
                                updateClock(packet.getClock());
                                LOGGER.info("Current VectorClock: " + clock);
                                lock.unlock();
                                break;
                            }
                        }
                        if (!foundExpected)
                            break;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
