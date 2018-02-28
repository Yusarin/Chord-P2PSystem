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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class CausalOrderProcess extends BlockingProcess {
    private final VectorClock clock;
    private final int clockPos;
    private final Lock lock = new ReentrantLock();
    private final Condition condition = lock.newCondition();

    public CausalOrderProcess(BlockingQueue q, int ID, ConcurrentHashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        super(q, ID, map, min_delay, max_delay);
        clock = new VectorClock(new int[map.size()]);// initialize to (0,0,0,0...)
        clockPos = ID - 1;
        super.deliverQueue = new PriorityBlockingQueue<Packet>(100);
        LOGGER.setLevel(Level.FINE);
        LOGGER.info("Current vector clock:" + clock);
    }

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
                    String[] parseDelay = parsed[1].split("\\s*delay\\s*", 2);
                    final Map delay;
                    HashMap<Integer, Long> customizeDelay = null;
                    byte[] msgBytes;
                    boolean random = true;
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
                        random = false;
                    } else {
                        msgBytes = parsed[1].getBytes();
                    }
                    delay = customizeDelay;
                    for (Map.Entry<Integer, InetSocketAddress> entry :
                            idMapIp.entrySet()) {
                        new Timer().schedule(new TimerTask() {
                            @Override
                            public void run() {
                                try {
                                    unicast_send(entry.getKey(), msgBytes, copyClock);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }, random ? (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay : (long) delay.get(entry.getKey()));
                    }//Send to everyone with different
                } else {
                    LOGGER.severe("not a legal command");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (NumberFormatException e) {
                LOGGER.severe("not a legal command");
            }
        }
    }

//    protected void causalSend(byte[] msg, VectorClock c) throws IOException {
//        for (Map.Entry<Integer, InetSocketAddress> entry :
//                idMapIp.entrySet()) {
//            unicast_send(entry.getKey(), msg, c);
//        }
//    }

    protected void unicast_send(int dst, byte[] msg, VectorClock c) throws IOException {
        LOGGER.finest("sending msg : " + new String(msg) + " to dst: " + dst);
        LOGGER.finest("sending Clock: " + c);
        Socket s;
        if (dst == selfID) {
            LOGGER.info("Receive Msg: " + new String(msg));
            return;
        }
        s = handleSendConnection(dst);
        LOGGER.finest("The socket is connected?: " + s.isConnected());
        int msg_len = msg.length;
        LOGGER.finest("msg length: " + msg_len);
        LOGGER.finest("sending to: " + s.getRemoteSocketAddress());
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        oos.flush();// TODO:Do we need flush?
        while (true) {
            try {
                oos.writeObject(new Packet(selfID, new String(msg), c));//Add a clock to the end
                break;
            } catch (Exception e) {
                e.printStackTrace();
                continue;// if network is wrong, keep sending
            }
        }
    }

    @Override
    protected void unicast_receive(int dst, byte[] msg) throws IOException {
        Socket s = idMapSocket.get(dst);
        LOGGER.finest("listening to process " + s.getRemoteSocketAddress());
        while (true) {
            Packet p = null;
            ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
            try {
                p = (Packet) ois.readObject();
                LOGGER.finest("get new packet");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            deliverQueue.add(p);
            lock.lock();
            condition.signal();
            lock.unlock();
        }
    }
}