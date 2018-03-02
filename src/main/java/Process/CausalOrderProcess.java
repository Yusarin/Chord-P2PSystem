package Process;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;

public class CausalOrderProcess extends BlockingProcess {
    private final VectorClock clock;
    /**
     * The clock position represent this process
     */
    private final int clockPos;
    /**
     * The clock shared between multiple threads, so we need a lock
     */
    private final Lock lock = new ReentrantLock();

    /**
     * Correspond condition variable
     */
    private final Condition condition = lock.newCondition();

    private final Lock writeLock = new ReentrantLock();

    public CausalOrderProcess(BlockingQueue<String> q, int ID, ConcurrentHashMap<Integer, InetSocketAddress> map,
            int min_delay, int max_delay) throws IOException {
        super(q, ID, map, min_delay, max_delay);
        clock = new VectorClock(new int[map.size()]);// initialize to (0,0,0,0...)
        clockPos = ID - 1;
        super.deliverQueue = new PriorityBlockingQueue<Packet>(100);
        LOGGER.setLevel(Level.INFO);
        LOGGER.info("Current vector clock:" + clock);
    }

    /**
     * This thread spawn three new thread
     * 1.Accept new connection
     * 2.Deliver packet
     * 3.Send packet
     */
    @Override
    public void run() {
        startAcceptingThread();
        new Thread(new DeliverThread(deliverQueue, clock, lock, condition, LOGGER)).start();// start a DeliverThread
        while (true) {
            try {
                final String msg = (String) writeQueue.take();
                String parsed[] = msg.split(" ", 2);
                if (parsed[0].equals("sleep")) {
                    Thread.sleep(1000);
                } else if (parsed[0].equals("clock")) {
                    LOGGER.info("Current vector clock: " + clock);
                } else if (parsed[0].equals("msend")) {
                    LOGGER.finest("Casual multicast");
                    String[] parseDelay = parsed[1].split("\\s+delay\\s+", 2);
                    HashMap<Integer, Long> customizeDelay = null;
                    byte[] msgBytes;
                    VectorClock copyClock;
                    synchronized (lock) {
                        clock.incAt(clockPos);
                        copyClock = clock.copy();
                        LOGGER.info("Current vector clock: " + clock);
                    }
                    if (parseDelay.length == 2) {
                        DelayParser delayParser = new DelayParser(parseDelay[1], idMapIp.size(), selfID);
                        customizeDelay = delayParser.parse();
                        msgBytes = parseDelay[0].getBytes();
                    } else {
                        msgBytes = parsed[1].getBytes();
                    }
                    causalSend(msgBytes, customizeDelay, copyClock);
                } else {
                    throw new IllegalArgumentException();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                LOGGER.severe("not a legal command");
            }
        }
    }

    /**
     * This function handle causal multicast send
     *
     * @param msg   message
     * @param delay Map from id to delay time
     * @param c     the copy of clock to attach to the packet
     */
    protected void causalSend(byte[] msg, HashMap<Integer, Long> delay, VectorClock c) {
        for (Map.Entry<Integer, InetSocketAddress> entry : idMapIp.entrySet()) {
            new Timer().schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        unicast_send(entry.getKey(), msg, c);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }, delay == null ? (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay
                    : delay.get(entry.getKey()));
        } //Send to everyone with different
    }

    protected void unicast_send(int dst, byte[] msg, VectorClock c) throws IOException {
        LOGGER.finest("sending msg : " + new String(msg) + " to dst: " + dst);
        LOGGER.finest("sending Clock: " + c);
        Packet p = new Packet(selfID, new String(msg), c);
        if (dst == selfID) {
            LOGGER.info("Received message: (" + p.getMsg() + ") from process: " + p.getSenderId() + " at system time: "
                    + new Date());
            LOGGER.info("Current VectorClock: " + clock);
            return;
        }
        ObjectOutputStream oos;
        oos = handleSendConnection(dst);
        while (true) {
            try {
                writeLock.lock();
                oos.writeObject(p);
                oos.flush();// TODO:Do we need flush?
                break;
            } catch (Exception e) {
                e.printStackTrace();
                continue;// if network is bad, keep sending until the packet sent successfully.
            } finally {
                writeLock.unlock();
            }
        }
    }

    /**
     * Override the unicast_receive to accommodate vector clock
     *
     * @param dst destination of receive
     * @param msg useless here
     * @throws IOException
     */
    @Override
    protected void unicast_receive(int dst, byte[] msg) throws IOException {
        Socket s = idMapSocket.get(dst);
        LOGGER.finest("listening to process " + s.getRemoteSocketAddress());
        ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
        while (true) {
            Packet p = null;
            try {
                p = (Packet) ois.readObject();
                LOGGER.finest("get new packet");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            deliverQueue.add(p);
        }
    }
}