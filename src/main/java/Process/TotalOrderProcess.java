package Process;

import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.*;

public class TotalOrderProcess extends BlockingProcess{

    private PriorityQueue FIFO_Buffer;
    private int sequence_cursor;

    public TotalOrderProcess() throws IOException {
        super(BlockingQueue q, int ID, HashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay);
        FIFO_Buffer = new PriorityQueue<String[]>(10，new Comparater<String[]>(){
            @Override
            public int compare(String[] s1, String[] s2){
                return Integer.parseInt(s2[1]) - Integer.parseInt(s1[1]);
            }
        });
        sequence_cursor = 0;
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
                                    multicast_receive(ipMapId.get(s.socket().getRemoteSocketAddress()), new byte[8]);
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

    private String multicast_receive(int dst, byte[] msg) throws IOException {
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

            String str = new String(message);
            String[] strs = str.split(",");
            if(Integer.parseInt(strs[1]) > this.sequence_cursor){
                FIFO_Buffer.offer(strs);
            }else{
                content.get(strs[2]);
                this.sequence_cursor++;
                for(String[] s = FIFO_Buffer.peek() ; s!=null && Integer.parseInt(s[1]) <= this.sequence_cursor ;this.sequence_cursor++){
                    String[] cur = FIFO_Buffer.pop();
                    content.get(cur[2]);
                }
            }

        }

        reset_master();
    }




}