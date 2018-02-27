package Process;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PipedOutputStream;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.net.ConnectException;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockingProcess implements Runnable {
    protected final BlockingQueue writeQueue;
    protected final HashMap<Integer, SocketChannel> idMapSocket = new HashMap<>();//map id to socket
    protected final HashMap<InetSocketAddress, Integer> ipMapId;//map ip to id
    protected final HashMap<Integer, InetSocketAddress> idMapIp;//map id to ip
    protected final BlockingQueue deliverQueue = new LinkedBlockingQueue<Packet>(100);
    protected final int ID;
    protected final InetSocketAddress addr;
    protected final ServerSocketChannel sock;
    protected final int min_delay;
    protected final int max_delay;

    public BlockingProcess(BlockingQueue q, int ID, HashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        this.addr = map.get(ID);
        sock = ServerSocketChannel.open();
        sock.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        sock.setOption(StandardSocketOptions.SO_REUSEPORT, true);
        sock.bind(this.addr);
        this.ID = ID;
        this.writeQueue = q;
        this.max_delay = max_delay;
        this.min_delay = min_delay;
        idMapIp = map;
        ipMapId = reverseMap(idMapIp);
    }

    //    public BlockingProcess(BlockingQueue q, int ID, HashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay, Comparator c) throws IOException {
//        this(q, ID, map, min_delay, max_delay);
//        this.deliverQueue = new PriorityBlockingQueue(100, c);
//    }
    protected void startAcceptingThread() {
        new Thread(() -> {
            while (true) {
                try {
                    SocketChannel s = sock.accept();
                    System.out.println("accepting: " + s.socket().getRemoteSocketAddress() + " is connected? " + s.isConnected());
                    if (!idMapSocket.containsValue(s)) {
                        Integer newID = ipMapId.get(s.socket().getRemoteSocketAddress());
                        System.out.println("incoming id: " + newID);
                        assert newID != null;
                        idMapSocket.put(newID, s);
                    } else {
                        System.out.println("Already accepted!");
                    }
                    new Thread(() -> {
                        try {
                            unicast_receive(ipMapId.get(s.socket().getRemoteSocketAddress()), new byte[8]);
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }).start();
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
                final String msg = (String) writeQueue.poll(1, TimeUnit.DAYS);
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            System.out.println("delay is: " + delay);
                            String parsed[] = msg.split(" ", 3);
                            if (parsed.length != 3) {
                                System.out.println("not a legal command");
                                return;
                            }
                            if (parsed[0].equals("send")) {
                                if (idMapIp.containsKey(Integer.parseInt(parsed[1]))) {
                                    unicast_send(Integer.parseInt(parsed[1]), parsed[2].getBytes());
                                }
                            } else {
                                System.out.println("not a legal command");
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

    protected void unicast_send(int dst, byte[] msg) throws IOException {
        System.out.println("sending msg : " + new String(msg) + " to dst: " + dst);
        SocketChannel s;
        if (dst == ID) {
            System.out.println("You are sending message to yourself! Msg: " + new String(msg));
            return;
        }
        if (idMapSocket.containsKey(dst)) {
            System.out.println("already contain key");
            s = idMapSocket.get(dst);
        } else {//this is first time connection
            s = SocketChannel.open();
            s.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            s.bind(addr);
            s.connect(idMapIp.get(dst));
            idMapSocket.put(dst, s);
            new Thread(() -> {
                try {
                    unicast_receive(ipMapId.get(s.socket().getRemoteSocketAddress()), new byte[8]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        System.out.println("The socket is connected?: " + s.isConnected());
        int msg_len = msg.length;
        System.out.println("msg length: " + msg_len);
        System.out.println("sending to: " + s.socket().getRemoteSocketAddress());
        ObjectOutputStream oos = new ObjectOutputStream(s.socket().getOutputStream());
        System.out.println("Object not send yet");
//        oos.writeInt(1);
        oos.flush();
        oos.writeObject(new Packet(new String(msg)));
        System.out.println("Object sent!");
    }

    protected void unicast_receive(int dst, byte[] msg) throws IOException {
        SocketChannel s = idMapSocket.get(dst);
        System.out.println("listening to process " + s.socket().getRemoteSocketAddress());
        while (true) {
            Packet p = null;
            ObjectInputStream ois = new ObjectInputStream(s.socket().getInputStream());
            try {
                System.out.println("aaaaaaaa");
                p = (Packet) ois.readObject();
                System.out.println("bbbbbbbb");
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
