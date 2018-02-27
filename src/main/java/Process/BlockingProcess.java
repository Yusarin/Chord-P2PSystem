package Process;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BlockingProcess implements Runnable {
    protected final BlockingQueue writeQueue;
    protected final HashMap<Integer, Socket> idMapSocket = new HashMap<>();//map id to socket
    protected final HashMap<InetSocketAddress, Integer> ipMapId;//map ip to id
    protected final HashMap<Integer, InetSocketAddress> idMapIp;//map id to ip
    protected BlockingQueue deliverQueue = new LinkedBlockingQueue<Packet>(100);
    protected final int selfID;
    protected final InetSocketAddress addr;
    protected final ServerSocket sock;
    protected final int min_delay;
    protected final int max_delay;
    protected final Lock mapAccessLock = new ReentrantLock();
    protected static final Logger LOGGER = Logger.getLogger(CausalOrderProcess.class.getName());

    public BlockingProcess(BlockingQueue q, int selfID, HashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        this.addr = map.get(selfID);
        sock = new ServerSocket();
        sock.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        sock.setOption(StandardSocketOptions.SO_REUSEPORT, true);
        sock.bind(this.addr);
        this.selfID = selfID;
        this.writeQueue = q;
        this.max_delay = max_delay;
        this.min_delay = min_delay;
        idMapIp = map;
        ipMapId = reverseMap(idMapIp);
        LOGGER.setLevel(Level.FINEST);
        LOGGER.info("Self PID: " + selfID);
    }

    protected void startAcceptingThread() {
        new Thread(() -> {
            while (true) {
                try {
                    Socket s = sock.accept();
                    Integer newID;
                    System.out.println("accepting: " + s.getRemoteSocketAddress() + " is connected? " + s.isConnected());
                    if (!idMapSocket.containsValue(s)) {
                        synchronized (mapAccessLock) {
                            newID = ipMapId.get(s.getRemoteSocketAddress());
                            System.out.println("incoming id: " + newID);
                            assert newID != null;
                            idMapSocket.put(newID, s);
                        }
                        new Thread(() -> {
                            try {
                                unicast_receive(newID, new byte[8]);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }).start();
                    } else {
                        throw new ConnectException("Already accept");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        System.out.println("accepting thread up");
    }

    @Override
    public void run() {
        System.out.println("server is up");
        System.out.println("listening on " + sock);
        startAcceptingThread();
        new Thread(new DeliverThread(deliverQueue, null)).start();// start a DeliverThread
        while (true) {
            try {
                final String msg = (String) writeQueue.take();
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                System.out.println("delay is: " + delay);
                String parsed[] = msg.split(" ", 3);
                if (parsed.length != 3) {
                    System.out.println("not a legal command");
                    continue;
                }
                if (parsed[0].equals("send")) {
                    if (idMapIp.containsKey(Integer.parseInt(parsed[1]))) {
                        new Timer().schedule(new TimerTask() {
                            @Override
                            public void run() {
                                try {
                                    unicast_send(Integer.parseInt(parsed[1]), parsed[2].getBytes());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }, delay);
                    } else {
                        LOGGER.warning("This PID is not exist");
                    }
                } else {
                    LOGGER.severe("not a legal command");
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    protected final Socket handleSendConnection(int dst) throws IOException {
        Socket s;
        if (idMapSocket.containsKey(dst)) {
            System.out.println("already contain key");
            s = idMapSocket.get(dst);
        } else {//this is first time connection
            s = new Socket();
            s.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            s.bind(addr);
            InetSocketAddress id;
            synchronized (mapAccessLock) {
                idMapSocket.put(dst, s);
                id = idMapIp.get(dst);
            }
            s.connect(id);
            new Thread(() -> {
                try {
                    unicast_receive(ipMapId.get(s.getRemoteSocketAddress()), new byte[8]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        return s;
    }


    protected void unicast_send(int dst, byte[] msg) throws IOException {
        System.out.println("sending msg : " + new String(msg) + " to dst: " + dst);
        Socket s;
        if (dst == selfID) {
            System.out.println("You are sending message to yourself! Msg: " + new String(msg));
            return;
        }
        s = handleSendConnection(dst);
        int msg_len = msg.length;
        System.out.println("msg length: " + msg_len);
        System.out.println("sending to: " + s.getRemoteSocketAddress());
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        oos.flush();// TODO:Do we need flush?
        oos.writeObject(new Packet(selfID, new String(msg)));
    }

    protected void unicast_receive(int dst, byte[] msg) throws IOException {
        Socket s;
        synchronized (mapAccessLock) {
            s = idMapSocket.get(dst);
        }
        System.out.println("listening to process " + s.getRemoteSocketAddress());
        while (true) {
            Packet p = null;
            ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
            try {
                p = (Packet) ois.readObject();
                System.out.println("get new packet");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            deliverQueue.add(p);
        }
    }

    protected HashMap<InetSocketAddress, Integer> reverseMap(HashMap<Integer, InetSocketAddress> map) {
        HashMap<InetSocketAddress, Integer> map_r = new HashMap<>();
        for (Map.Entry<Integer, InetSocketAddress> e : map.entrySet()) {
            map_r.put(e.getValue(), e.getKey());
        }
        return map_r;
    }
}
