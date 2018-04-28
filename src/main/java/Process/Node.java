package Process;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.StandardSocketOptions;
import java.util.Random;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class Node extends BlockingProcess{

    public Node(BlockingQueue q, int ID, ConcurrentHashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        super(q, ID, map, min_delay, max_delay);
        int port_offset = map.get(selfID).getPort() - selfID;
        for(int i = 0 ; i < 256 ; i++){
            idMapIp.put(i, new InetSocketAddress("127.0.0.1", port_offset+i));
        }

        this.addr = idMapIp.get(selfID);
        ipMapId = reverseMap(idMapIp);
        this.sock.bind(this.addr);
        this.Alive = true;
    }

    @Override
    public void run() {

        System.out.println("Node is up");
        System.out.println("My ip is");
        System.out.println(this.addr);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Socket s = sock.accept();
                        System.out.println("accepting: " + s.getRemoteSocketAddress() + " is connected? " + s.isConnected());
                        System.out.println("Listening from " + s.getRemoteSocketAddress());
                        if (!idMapOOS.containsValue(s)) {
                            Integer newID = ipMapId.get(s.getRemoteSocketAddress());
                            System.out.println("incoming id: " + newID);
                            assert newID != null;
                            idMapSocket.put(newID, s);
                            idMapOOS.put(newID, new ObjectOutputStream(s.getOutputStream()));
                        }
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    unicast_receive(ipMapId.get(s.getRemoteSocketAddress()));
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

        try {
            join(0);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while(true){
            heartbeat();
        }
    }

    /**
     * This function handles connection (client side). If this is the first message, the new established
     * Socket need to be added to global maps. Otherwise, it just pull out the record from the map.
     *
     * @param dst
     * @return
     * @throws IOException
     */
    public ObjectOutputStream NhandleSendConnection(int dst) throws IOException {
        Socket s;
        ObjectOutputStream oos = null;
        if (idMapOOS.containsKey(dst)) {
            oos = idMapOOS.get(dst);
        } else {//this is first time connection
            s = new Socket();
            s.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            s.bind(addr);
            InetSocketAddress ip;
            ip = idMapIp.get(dst);
            s.connect(ip);
            oos = new ObjectOutputStream(s.getOutputStream());
            idMapSocket.put(dst, s);
            idMapOOS.put(dst, oos);
            new Thread(() -> {
                try {
                    unicast_receive(ipMapId.get(s.getRemoteSocketAddress()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        return oos;
    }

    public void heartbeat(){
        try {
            Thread.sleep(this.Heartbeat_Period*100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        String msg = selfID+" Alive";
        try {
            unicast_send(0, msg.getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}
