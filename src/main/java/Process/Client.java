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

public class Client extends BlockingProcess{
    Integer alloc_port;
    String additional_msg;
    ConcurrentHashMap<Integer, String> showall = new ConcurrentHashMap<>();
    public Client(BlockingQueue q, int ID, ConcurrentHashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        super(q, ID, map, min_delay, max_delay);
        this.alloc_port = (int)(3000 + Math.random()*12000);
        additional_msg = "";

        for(int i = 0 ; i < 256 ; i++)
            this.idMapIp.put(i, new InetSocketAddress("127.0.0.1", this.alloc_port+i));

        //Keys 0-255 Initially stored at Node 0.
        for(int i = 0 ; i < 256 ; i++){
            Local_Keys.add(i);
        }

        //Initialize finger table at Node 0.
        for(int i = 0; i < 8 ; i++){
            Finger_table.put(i,0);
        }
    }

    @Override
    public void run() {
        System.out.println("Client is up");
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
                                    client_receive(ipMapId.get(s.getRemoteSocketAddress()));
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
                String parsed[] = msg.split(" ");

                if(parsed[0].equals("join")){
                    //Initialize newnode.
                    int newnode = Integer.parseInt(parsed[1]);
                    if (newnode != 0) {
                        try {
                            ConcurrentHashMap<Integer, InetSocketAddress> m = new ConcurrentHashMap<>(this.idMapIp);
                            alloc_port += newnode;
                            m.put(newnode, new InetSocketAddress("127.0.0.1", alloc_port + newnode));
                            this.running.put(newnode, new InetSocketAddress("127.0.0.1", alloc_port + newnode));
                            additional_msg = " "+alloc_port +" "+ newnode;
                            // For Nodes, this map m will only contain its own value. For client, this map is responsible for all Nodes.
                            new Thread(new Node(new LinkedBlockingDeque<String>(), newnode, m, min_delay, max_delay)).start();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    } else {
                        System.out.println("Node 0 is the client node. Choose another identifier from 1-255");
                        continue;
                    }



                } else if (parsed[0].equals("find")) {

                    int initiate_node = Integer.parseInt(parsed[1]);
                    if(!idMapIp.containsKey(initiate_node)){
                        System.out.println("Node doesn't exist.");
                    } else {
                        try{
                            client_send(initiate_node, new Message(selfID, addr, msg+additional_msg));
                        }
                        catch(IOException e){
                            e.printStackTrace();
                        }
                    }

                } else if (parsed[0].equals("crash")) {

                    int crash = Integer.parseInt(parsed[1]);
                    if(!idMapIp.containsKey(crash)){
                        System.out.println("Node doesn't exist.");
                    } else {
                        try{
                            client_send(crash, new Message(selfID, addr, msg+additional_msg));
                        }
                        catch(IOException e){
                            e.printStackTrace();
                        }
                    }

                } else if (parsed[0].equals("show")) {

                    if(parsed[1].equals("all")){
                        additional_msg = "";
                        for (int i : running.keySet()) {
                            final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                            new Timer().schedule(new TimerTask() {
                                @Override
                                public void run() {
                                    try {
                                        if (i != selfID)
                                            client_send(i, new Message(selfID, addr, msg + additional_msg));
                                    } catch (IOException e) {
                                        e.printStackTrace();
                                    }
                                }
                            }, delay);
                        }
                    } else {
                        int showid = Integer.parseInt(parsed[1]);
                        if (!idMapIp.containsKey(showid)) {
                            System.out.println("Node doesn't exist.");
                        } else {
                            try {
                                client_send(showid, new Message(selfID, addr, msg + additional_msg));
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                        continue;
                    }

                } else {
                    System.out.println("Illegal command.");
                    continue;
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
    public ObjectOutputStream ChandleSendConnection(int dst) throws IOException {
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
                    client_receive(ipMapId.get(s.getRemoteSocketAddress()));
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
     * @throws IOException
     */
    public void client_receive(int dst) throws IOException {
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

            String[] msgs = strmsg.split(",");
            String real_msg = msgs[1];
            if(real_msg.startsWith("show")){

                String message = "s-show_fing ";
                String table = "";
                String keys = "";
                if(real_msg.charAt(5) == 'a'){
                    message = "a-show_fing ";
                }
                for(int i : Finger_table.keySet()){
                    table += i+":";
                    table += Finger_table.get(i)+",";
                }
                table = table.substring(0,table.length()-1);

                for(int i : Local_Keys){
                    keys += ",";
                }
                keys = keys.substring(0, keys.length()-1);

                message += table + " ";
                message += keys;
                unicast_send(0, message.getBytes());

            } else if(real_msg.startsWith("crash")){
                //TODO: implement crash.
            } else if(real_msg.startsWith("find")){
                //TODO: implement find.
            } else if(real_msg.startsWith("succ")){

                String message = "resp_succ ";
                message += this.successor;
                unicast_send(dst, message.getBytes());

            } else if(real_msg.startsWith("pred")){

                String message = "resp_pred ";
                message += this.successor;
                unicast_send(dst, message.getBytes());

            } else if(real_msg.startsWith("fing")){

                String message = "resp_fing ";
                String table = "";
                for(int i : Finger_table.keySet()){
                    table += i+",";
                    table += Finger_table.get(i)+"#";
                }
                message += table;
                unicast_send(dst, message.getBytes());

            } else if(real_msg.startsWith("keys")){

                String message = "resp_keys ";
                String set = "";
                for(int i : Local_Keys){
                    set += i+"#";
                }
                message += set;
                unicast_send(dst, message.getBytes());

            } else if(real_msg.startsWith("setPred")){

                String[] strs = real_msg.split(" ");
                this.predecessor = Integer.parseInt(strs[1]);

            } else if(real_msg.startsWith("setSucc")){

                String[] strs = real_msg.split(" ");
                this.successor = Integer.parseInt(strs[1]);

            } else if(real_msg.startsWith("update_finger_table")){

                String[] strs = real_msg.split(" ");
                update_finger_table(Integer.parseInt(strs[1]), Integer.parseInt(strs[2]));

            } else if(real_msg.startsWith("resp_succ")){

                String[] strs = real_msg.split(" ");
                wait_succ = strs[1];

            } else if(real_msg.startsWith("resp_pred")){

                String[] strs = real_msg.split(" ");
                wait_pred = strs[1];

            } else if(real_msg.startsWith("resp_fing")){

                String[] strs = real_msg.split(" ");
                wait_fin = strs[1];

            } else if(real_msg.startsWith("resp_keys")){

                String[] strs = real_msg.split(" ");
                wait_keys = strs[1];

            } else if(real_msg.startsWith("s-show_fing")){

                String[] strs = real_msg.split(" ");
                System.out.println(dst);
                System.out.println(strs[1]);
                System.out.println(strs[2]);

            } else if(real_msg.startsWith("a-show_fing")){

                //Print record from node 0.
                System.out.println(0);
                String mytable = "";
                for(int i : Finger_table.keySet()){
                    mytable += i+":";
                    mytable += Finger_table.get(i)+",";
                }
                System.out.println(mytable.substring(0, mytable.length()-1));

                String set = "";
                for(int i : Local_Keys){
                    set += i+",";
                }
                System.out.println(set.substring(0, set.length()-1));

                String[] strs = real_msg.split(" ");
                showall.put(dst, strs[1] + " " + strs[2]);

                if(showall.size() == running.size()){
                    for(int i : showall.keySet()){
                        System.out.println(i);
                        String[] ss = showall.get(i).split(" ");
                        System.out.println(ss[0]);
                        System.out.println(ss[1]);
                    }
                }

            }
        }
    }

    /**
     * Send message to the corresponding process with ID
     *
     * @param dst
     * @param m
     * @throws IOException
     */
    private void client_send(int dst, Message m) throws IOException {
        System.out.println("sending msg : " + m.msg + " to dst: " + dst);
        ObjectOutputStream oos = ChandleSendConnection(dst);
        oos.flush();// TODO:Do we need flush.
        oos.writeObject(new Message(m.Sender_ID, m.Sender_addr, m.msg));
    }

}
