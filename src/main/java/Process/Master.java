package Process;

import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.*;

class Message{
    int Sender_ID;
    InetSocketAddress Sender_addr;
    String msg;

    public Message(int ID, InetSocketAddress addr, String msg){
        this.Sender_ID = ID;
        this.Sender_addr = addr;
        this.msg = msg;
    }
}

public class Master extends BlockingProcess{
    Queue<Message> sequence;
    public Master() throws IOException {
        super(BlockingQueue q, int ID, HashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay);
        this.sequence = new LinkedList<Message>();
    }

    @Override
    public void run() {
        System.out.println("sequencer is up");
        //System.out.println("listening on " + sock);
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
                //Sending messages in the queue to all other processes in FIFO order.
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            //Sending message to all processes.
                            while(!sequence.isEmpty()){
                                Message current = sequence.poll();
                                for(int i : idMapSocket.keySet()){
                                    unicast_send(i, current.msg.getByte());
                                }
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

    @Override
    private String unicast_receive(int dst, byte[] msg) throws IOException {
        SocketChannel s = idMapSocket.get(dst);
        while (true) {
            ByteBuffer sizeBuf = ByteBuffer.allocate(4);
            s.read(sizeBuf);
            int length = sizeBuf.flip().getInt();
            System.out.println("receive " + length + " bytes");
            ByteBuffer content = ByteBuffer.allocate(length);
            s.read(content);
            content.flip();
            byte[] message = new byte[content.remaining()];
            content.get(message);
            
            //Adding every message into the sequencer in FCFS order.
            String strmsg = new String(message);
            Message m = new Message(ipMapID.get(s.socket().getRemoteSocketAddress(), s.socket().getRemoteSocketAddress(), strmsg));
            sequence.offer(m);
            System.out.println("Sequencer Received: " + strmsg);
        }
    }

}
