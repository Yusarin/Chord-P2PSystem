package Process;

import java.io.*;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class TotalOrderProcess extends BlockingProcess {

    private PriorityQueue FIFO_Buffer;
    private int sequence_cursor;

    public TotalOrderProcess(BlockingQueue q, int ID, ConcurrentHashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        super(q, ID, map, min_delay, max_delay);
        FIFO_Buffer = new PriorityQueue<String[]>(10, new Comparator<String[]>() {
            @Override
            public int compare(String[] s1, String[] s2) {
                return Integer.parseInt(s1[1]) - Integer.parseInt(s2[1]);
            }
        });
        sequence_cursor = 1;
    }

    @Override
    public void run() {
        System.out.println("A TotalOrderProcess is up");
        //System.out.println("listening on " + sock);
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Socket s = sock.accept();
                        System.out.println("accepting: " + s.getRemoteSocketAddress() + " is connected? " + s.isConnected());
                        if (!idMapSocket.containsValue(s)) {
                            Integer newID = ipMapId.get(s.getRemoteSocketAddress());
                            System.out.println("incoming id: " + newID);
                            assert newID != null;
                            idMapSocket.put(newID, s);
                        }
                        new Thread(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    multicast_receive(ipMapId.get(s.getRemoteSocketAddress()), new byte[8]);
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
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                final String msg = (String) writeQueue.poll(1, TimeUnit.DAYS);
                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            //Send message to Master.
                            System.out.println("delay is :" + delay);
                            String parsed[] = msg.split(" ", 2);
                            if (parsed.length != 2) {
                                System.out.println("not a legal command");
                                return;
                            }
                            if (parsed[0].equals("msend")) {

                                multicast_send(0, parsed[1].getBytes());
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

    public Socket MhandleSendConnection(int dst) throws IOException {
        Socket s;
        if (idMapSocket.containsKey(dst)) {
            s = idMapSocket.get(dst);
        } else {//this is first time connection
            s = new Socket();
            s.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            s.bind(addr);
            InetSocketAddress id;
            idMapSocket.put(dst, s);
            id = idMapIp.get(dst);
            s.connect(id);
            new Thread(() -> {
                try {
                    multicast_receive(ipMapId.get(s.getRemoteSocketAddress()), new byte[8]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        return s;
    }


    protected void multicast_send(int dst, byte[] msg) throws IOException {
        //System.out.println("sending msg : " + new String(msg) + " to dst: " + dst);
        Socket s;
        if (dst == selfID) {
            System.out.println("You are sending message to yourself! Msg: " + new String(msg));
            return;
        }
        s = MhandleSendConnection(dst);
        int msg_len = msg.length;
        //System.out.println("msg length: " + msg_len);
        //System.out.println("sending to: " + s.getRemoteSocketAddress());
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        oos.flush();// TODO:Do we need flush?
        oos.writeObject(new Message(selfID, idMapIp.get(dst), new String(msg), 0));
    }

    private String multicast_receive(int dst, byte[] msg) throws IOException {
        Socket s = idMapSocket.get(dst);
        while (true) {
            ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
            Message m = null;
            try {
                m = (Message) ois.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            String str = m.Serial;
            System.out.println(str);
            String[] strs = str.split(",");
            if (Integer.parseInt(strs[1]) > this.sequence_cursor) {
                //System.out.println("Buffering message");
                FIFO_Buffer.offer(strs);
            } else {
                System.out.println("Reveive Message " + strs[2] + "From Process "+ m.Sender_ID + "at time "+ Calendar.getInstance().getTime());
                this.sequence_cursor++;
                for (String[] tmps = (String[]) FIFO_Buffer.peek(); tmps != null && Integer.parseInt(tmps[1]) <= this.sequence_cursor && !FIFO_Buffer.isEmpty(); this.sequence_cursor++) {
                    String[] cur = (String[]) FIFO_Buffer.poll();
                    System.out.println("Reveive Message " + cur[2] + "From Process "+ m.Sender_ID + "at time "+ Calendar.getInstance().getTime());
                }
            }
        }
    }
}
