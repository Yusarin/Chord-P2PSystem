package Process;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BlockingProcess implements Runnable {
    protected final BlockingQueue writeQueue;
    protected final Map<Integer, ObjectOutputStream> idMapOOS = new ConcurrentHashMap<>();//map id to socket
    protected final Map<Integer, Socket> idMapSocket = new ConcurrentHashMap<>();//map id to socket
    protected final Map<InetSocketAddress, Integer> ipMapId;//map ip to id
    protected final Map<Integer, InetSocketAddress> idMapIp;//map id to ip
    protected BlockingQueue deliverQueue = new LinkedBlockingQueue<Packet>(100);
    protected final int selfID;
    protected final InetSocketAddress addr;
    protected final ServerSocket sock;
    protected final int min_delay;
    protected final int max_delay;
    protected static final Logger LOGGER = Logger.getLogger(CausalOrderProcess.class.getName());


    public BlockingProcess(BlockingQueue q, int selfID, ConcurrentHashMap<Integer, InetSocketAddress> map,
                           int min_delay, int max_delay) throws IOException {
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

    /**
     * This thread will spawn a thread to listen to a socket if a new connection is accepted.
     */
    protected void startAcceptingThread() {
        new Thread(() -> {
            while (true) {
                try {
                    Socket s = sock.accept();
                    Integer newID;
                    LOGGER.finest("accepting: " + s.getRemoteSocketAddress() + " is connected? " + s.isConnected());
                    if (!idMapOOS.containsValue(s)) {
                        newID = ipMapId.get(s.getRemoteSocketAddress());
                        System.out.println("incoming id: " + newID);
                        assert newID != null;
                        idMapOOS.put(newID, new ObjectOutputStream(s.getOutputStream()));
                        idMapSocket.put(newID, s);
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

    /**
     * This thread spawn three new thread
     * 1.Accept new connection
     * 2.Deliver packet
     * 3.Send packet
     */
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
                String parsed[] = msg.split("\\s+", 3);
                if (parsed.length != 3)
                    throw new IllegalArgumentException();
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
     * This function handle connection (client side). If this is the first message, the new established
     * Socket need to be added to global maps. Otherwise, it just pull out the record from the map.
     *
     * @param dst
     * @return
     * @throws IOException
     */
    protected final ObjectOutputStream handleSendConnection(int dst) throws IOException {
        Socket s;
        ObjectOutputStream oos = null;
        if (idMapOOS.containsKey(dst)) {
            oos = idMapOOS.get(dst);
        } else {//this is first time connection
            s = new Socket();
            s.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            s.bind(addr);
            InetSocketAddress id;
            id = idMapIp.get(dst);
            s.connect(id);
            oos = new ObjectOutputStream(s.getOutputStream());
            idMapOOS.put(dst, oos);
            idMapSocket.put(dst, s);
            new Thread(() -> {
                try {
                    unicast_receive(ipMapId.get(s.getRemoteSocketAddress()), new byte[8]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        return oos;
    }

    /**
     * Handle unicast send. There is not much to say here.
     *
     * @param dst
     * @param msg
     * @throws IOException
     */
    protected void unicast_send(int dst, byte[] msg) throws IOException {
        System.out.println("sending msg : " + new String(msg) + " to dst: " + dst);
        ObjectOutputStream oos;
        oos = handleSendConnection(dst);
        Packet p = new Packet(selfID, new String(msg));
        if (dst == selfID) {
            deliverQueue.add(p);
            return;
        }
        oos.flush();// TODO:Do we need flush?
        oos.writeObject(p);
    }

    /**
     * Handle receive. Once see a packet, put the packet in the queue
     *
     * @param dst
     * @param msg
     * @throws IOException
     */
    protected void unicast_receive(int dst, byte[] msg) throws IOException {
        Socket s = idMapSocket.get(dst);
        System.out.println("listening to process " + s.getRemoteSocketAddress());
        ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
        while (true) {
            Packet p = null;
            try {
                p = (Packet) ois.readObject();
                System.out.println("get new packet");
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            deliverQueue.add(p);
        }
    }

    /**
     * A helper function to "reverse" a map.
     *
     * @param map
     * @return
     */
    protected static Map<InetSocketAddress, Integer> reverseMap(Map<Integer, InetSocketAddress> map) {
        Map<InetSocketAddress, Integer> map_r = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, InetSocketAddress> e : map.entrySet()) {
            map_r.put(e.getValue(), e.getKey());
        }
        return map_r;
    }
}
