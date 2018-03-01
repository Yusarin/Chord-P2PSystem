package Process;


import java.io.*;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class Master extends BlockingProcess {
    private int headercounter;
    private BlockingQueue<Message> sequence;
    private boolean isSending;

    public Master(BlockingQueue q, int ID, ConcurrentHashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        super(q, ID, map, min_delay, max_delay);
        this.headercounter = 0;
        this.sequence = new LinkedBlockingDeque<Message>(100);
        this.isSending = false;
    }

    @Override
    public void run() {
        System.out.println("Sequencer is up");
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
                                    master_receive(ipMapId.get(s.getRemoteSocketAddress()), new byte[8]);
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
            //Sending messages in the queue to all other processes in FIFO order.
            try {
                while (!sequence.isEmpty()) {
                    Message current = sequence.poll(1, TimeUnit.DAYS);

                    isSending = true;
                    for (int i : idMapIp.keySet()) {
                        final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                        new Timer().schedule(new TimerTask() {
                            @Override
                            public void run() {
                                try {
                                    if (i != selfID)
                                        master_send(i, current);

                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }, delay);
                    }
                    isSending = false;
                }
            }catch (InterruptedException e) {
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
            System.out.println(id);
            s.connect(id);
            new Thread(() -> {
                try {
                    master_receive(ipMapId.get(s.getRemoteSocketAddress()), new byte[8]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        return s;
    }

    public void master_receive(int dst, byte[] msg) throws IOException {
        Socket s = idMapSocket.get(dst);
        while (true) {
            Message m = null;
            ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
            try {
                m = (Message) ois.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            String strmsg = m.Serial;
            //Message m = new Message(ipMapId.get(s.socket().getRemoteSocketAddress()), (InetSocketAddress) s.socket().getRemoteSocketAddress(), strmsg, headercounter);
            if(!isSending) {
                this.headercounter++;
                System.out.println("header modified, currently is " + this.headercounter);
                m.header = this.headercounter;
                sequence.offer(new Message(m.Sender_ID, m.Sender_addr, m.msg, m.header));
            }
            System.out.println("Sequencer Received: " + strmsg);
        }
    }

    private void master_send(int dst, Message m) throws IOException {
        System.out.println("sending msg : " + m.msg + " to dst: " + dst);
        Socket s = MhandleSendConnection(dst);
        ObjectOutputStream oos = new ObjectOutputStream(s.getOutputStream());
        oos.flush();// TODO:Do we need flush.
        oos.writeObject(new Message(selfID, idMapIp.get(dst), m.msg, m.header));//TODO: message
    }

    public void reset_master() {
        this.headercounter = 0;
        this.sequence = new LinkedBlockingDeque<Message>();
    }
}
