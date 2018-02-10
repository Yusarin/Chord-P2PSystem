package Process;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class BlockingProcess implements Runnable {
    private BlockingQueue writeQueue;
    private HashMap<Integer, SocketChannel> idMapSocket = new HashMap<>();//map id to socket
    private HashMap<InetSocketAddress, Integer> ipMapId;//map ip to id
    private HashMap<Integer, InetSocketAddress> idMapIp;//map id to ip
    private int ID;
    private InetSocketAddress addr;
    private ServerSocketChannel sock;
    private int min_delay;
    private int max_delay;

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

    @Override
    public void run() {
        System.out.println("server is up");
        System.out.println("listening on " + sock);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        SocketChannel s = sock.accept();
                        System.out.println("accepting: " + s.socket().getRemoteSocketAddress() + " is connected? " + s.isConnected());
                        if (!idMapSocket.containsValue(s)) {
                            Integer newID = ipMapId.get(s.socket().getRemoteSocketAddress());
                            System.out.println("incoming id: " + newID);
                            assert newID != null;
                            idMapSocket.put(newID, s);
                        }
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    System.out.println("molea");
                                    unicast_receive(ipMapId.get(s.socket().getRemoteSocketAddress()), new byte[8]);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }).start();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
        while (true) {
            try {
                final String msg = (String) writeQueue.poll(1, TimeUnit.DAYS);
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            System.out.println("delay is :" + delay);
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

    private void unicast_send(int dst, byte[] msg) throws IOException {
        System.out.println("sending msg : " + new String(msg) + "to dst: " + dst);
        SocketChannel s;
        if (idMapSocket.containsKey(dst)) {
            System.out.println("already in known socket");
            s = idMapSocket.get(dst);
        } else {
            System.out.println("should connect to " + idMapIp.get(dst));
            s = SocketChannel.open();
            s.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            s.bind(addr);
            s.connect(idMapIp.get(dst));
            idMapSocket.put(dst, s);
        }
        int msg_len = msg.length;
        System.out.println("msg length: " + msg_len);
        s.write(ByteBuffer.allocate(4).putInt(msg_len).flip());
        s.write(ByteBuffer.wrap(msg));
    }

    private void unicast_receive(int dst, byte[] msg) throws IOException {
        SocketChannel s = idMapSocket.get(dst);
        while (true) {
            ByteBuffer sizeBuf = ByteBuffer.allocate(4);
            s.read(sizeBuf);
            int length = sizeBuf.flip().getInt();
            System.out.println("receive " + length + "bytes");
            ByteBuffer content = ByteBuffer.allocate(length);
            s.read(content);
            content.flip();
            byte[] message = new byte[content.remaining()];
            content.get(message);
            System.out.println("Received :" + new String(message));
        }
    }

    private HashMap<InetSocketAddress, Integer> reverseMap(HashMap<Integer, InetSocketAddress> map) {
        HashMap<InetSocketAddress, Integer> map_r = new HashMap<>();
        for (Map.Entry<Integer, InetSocketAddress> e : map.entrySet()) {
            map_r.put(e.getValue(), e.getKey());
        }
        return map_r;
    }
}
