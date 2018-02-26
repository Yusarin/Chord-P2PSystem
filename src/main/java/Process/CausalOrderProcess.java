package Process;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CausalOrderProcess extends BlockingProcess {
    private final VectorClock clock;
    private final int clockPos;
    private final BlockingQueue deliverQueue = new PriorityBlockingQueue(100);
    private static final Logger LOGGER = Logger.getLogger(CausalOrderProcess.class.getName());
    private final Object lock = new Object();

    public CausalOrderProcess(BlockingQueue q, int ID, HashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        super(q, ID, map, min_delay, max_delay);
        clock = new VectorClock(new int[map.size()]);// initialize to (0,0,0,0...)
        clockPos = ID - 1;
        LOGGER.setLevel(Level.FINEST);
        LOGGER.info("Current vector clock:" + clock);
    }

    @Override
    public void run() {
        startAcceptingThread();
        new Thread(new DeliverThread(deliverQueue, null, lock)).start();// start a DeliverThread
        while (true) {
            try {
                final String msg = (String) writeQueue.poll(1, TimeUnit.DAYS);
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            LOGGER.info("delay is: " + delay);
                            String parsed[] = msg.split(" ", 2);
                            if (parsed[0].equals("msend")) {
                                LOGGER.finest("Casual multicast");
                                causalSend(parsed[1].getBytes());
                            } else if (parsed[0].equals("clock")) {
                                LOGGER.info("Current vector clock: " + clock);
                            } else {
                                LOGGER.severe("not a legal command");
                            }
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }, delay);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected void causalSend(byte[] msg) throws IOException {
        synchronized (lock) {
            clock.incAt(clockPos);
            LOGGER.info("Current vector clock: " + clock);
        }
        for (Map.Entry<Integer, InetSocketAddress> entry :
                idMapIp.entrySet()) {
            unicast_send(entry.getKey(), msg);
        }//TODO: need to finish causalSend
    }
}