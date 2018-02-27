package Process;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.BlockingQueue;
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

    public CausalOrderProcess(BlockingQueue q, int ID, HashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
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
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                String parsed[] = msg.split(" ", 2);
                if (parsed[0].equals("clock")) {
                    LOGGER.info("Current vector clock: " + clock);
                } else if (parsed[0].equals("msend")) {
                    LOGGER.finest("Casual multicast");
                    new Timer().schedule(new TimerTask() {
                        @Override
                        public void run() {
                            try {
                                LOGGER.info("delay is: " + delay);
                                causalSend(parsed[1].getBytes());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }, delay);
                } else {
                    LOGGER.severe("not a legal command");
                }
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

    @Override
    protected void unicast_send(int dst, byte[] msg) throws IOException {
        LOGGER.finest("sending msg : " + new String(msg) + " to dst: " + dst);
        Socket s;
        if (dst == selfID) {
            LOGGER.finest("You are sending message to yourself! Msg: " + new String(msg));
            return;
        }
        s = handleSendConnection(dst);
        LOGGER.finest("The socket is connected?: " + s.isConnected());
        int msg_len = msg.length;
        LOGGER.finest("msg length: " + msg_len);
        LOGGER.finest("sending to: " + s.getRemoteSocketAddress());
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        oos.flush();// TODO:Do we need flush?
        oos.writeObject(new Packet(selfID, new String(msg), clock));
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