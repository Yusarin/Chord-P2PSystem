package Process;


import java.io.*;
import java.util.*;
import java.net.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class Master extends BlockingProcess {
    /**
     * A counter to keep track of the order of the sent process
     */
    private int headercounter;

    /**
     * A queue to store all receiving messages.
     */
    private BlockingQueue<Message> sequence;

    /**
     * A boolean flag to protect headercounter when sending, not necessary in this program.
     */
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

                    for (int i : idMapIp.keySet()) {
                        final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                        //System.out.println("customdelay equals " + current.customDelay);
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
                        }, current.customDelay == -1 ? delay : current.customDelay);
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

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
    public ObjectOutputStream MhandleSendConnection(int dst) throws IOException {
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
                    master_receive(ipMapId.get(s.getRemoteSocketAddress()), new byte[8]);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }).start();
        }
        return oos;
    }

    /**
     * Handle master receive, once called, receives message from all processes, once received a message, put it in the
     * queue and then update the header counter.
     *
     * @param dst
     * @param msg
     * @throws IOException
     */
    public void master_receive(int dst, byte[] msg) throws IOException {
        Socket s = idMapSocket.get(dst);
        ObjectInputStream ois = new ObjectInputStream(s.getInputStream());
        while (true) {
            Message m = null;
            try {
                m = (Message) ois.readObject();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            String strmsg = m.Serial;
            this.headercounter++;
            m.header = this.headercounter;
            sequence.offer(new Message(m.Sender_ID, m.Sender_addr, m.msg, m.header, m.customDelay));

            System.out.println("Sequencer Received: " + strmsg);
        }
    }

    /**
     * Send message to the corresponding process with ID
     *
     * @param dst
     * @param m
     * @throws IOException
     */
    private void master_send(int dst, Message m) throws IOException {
        System.out.println("sending msg : " + m.msg + " to dst: " + dst);
        ObjectOutputStream oos = MhandleSendConnection(dst);
        oos.flush();// TODO:Do we need flush.
        oos.writeObject(new Message(m.Sender_ID, m.Sender_addr, m.msg, m.header, m.customDelay));
    }

    /**
     * Reset the master node.
     */
    public void reset_master() {
        this.headercounter = 0;
        this.sequence = new LinkedBlockingDeque<Message>();
    }
}
