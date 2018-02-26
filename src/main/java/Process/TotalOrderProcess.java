package test;

import java.io.*;
import java.nio.channels.ServerSocketChannel;
import java.util.*;
import java.net.*;
import java.nio.*;
import java.nio.channels.SocketChannel;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public class TotalOrderProcess extends BlockingProcess{

    private PriorityQueue FIFO_Buffer;
    private int sequence_cursor;
    private BlockingQueue writeQueue;
    private HashMap<Integer, SocketChannel> idMapSocket = new HashMap<>();//map id to socket
    private HashMap<InetSocketAddress, Integer> ipMapId;//map ip to id
    private HashMap<Integer, InetSocketAddress> idMapIp;//map id to ip
    private int ID;
    private InetSocketAddress addr;
    private ServerSocketChannel sock;
    private int min_delay;
    private int max_delay;

    public TotalOrderProcess(BlockingQueue q, int ID, HashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        super(q, ID, map,min_delay, max_delay);
        FIFO_Buffer = new PriorityQueue<String[]>(10, new Comparator<String[]>(){
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

                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                final String msg = (String) writeQueue.poll(1, TimeUnit.DAYS);
                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            //Send message to Master.
                            System.out.println("delay is :" + delay);
                            String parsed[] = msg.split(" ", 3);
                            if (parsed.length != 3) {
                                System.out.println("not a legal command");
                                return;
                            }
                            if (parsed[0].equals("send")) {
                                if (idMapIp.containsKey(Integer.parseInt(parsed[1]))) {
                                    unicast_send(0, parsed[2].getBytes());
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
                content.get(strs[2].getBytes());
                this.sequence_cursor++;
                for(String[] tmps = (String[])FIFO_Buffer.peek() ; tmps!=null && Integer.parseInt(tmps[1]) <= this.sequence_cursor ;this.sequence_cursor++){
                    String[] cur = (String[])FIFO_Buffer.poll();
                    content.get(cur[2].getBytes());
                }
            }
        }
    }
}
