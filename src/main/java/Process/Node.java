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
    }

    @Override
    public void run() {

        System.out.println("Node is up");
        new Thread(new Runnable() {
            @Override
            public void run() {
                while (true) {
                    try {
                        Socket s = sock.accept();
                        System.out.println("accepting: " + s.getRemoteSocketAddress() + " is connected? " + s.isConnected());
                        System.out.println("Listening from "+s.getRemoteSocketAddress());
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
//        while (true) {
//            //Sending messages in the queue to all other processes in FIFO order.
//            try {
//                final String msg = (String) writeQueue.poll(1, TimeUnit.DAYS);
//                String parsed[] = msg.split(" ");
//                if(parsed[0].equals("join")){
//                    int newnode = Integer.parseInt(parsed[1]);
//                    if (newnode != 0) {
//                        new Thread(new Node(q, newnode, config.m, min_delay, max_delay)).start();
//                    }
//                } else if (parsed[0].equals("find")) {
//                    //TODO:Implement find.
//                } else if (parsed[0].equals("crash")) {
//                    //TODO:Implement crash.
//                } else if (parsed[0].equals("show")) {
//                    //TODO:Implement show.
//                } else {
//                    System.out.println("Illegal command.");
//                    continue;
//                }
//                for (int i : idMapIp.keySet()) {
//                    final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
//                    new Timer().schedule(new TimerTask() {
//                        @Override
//                        public void run() {
//                            try {
//                                if (i != selfID)
//                                    client_send(i, new Message(i, idMapIp.get(i), msg)); //TODO:specify message to be send.
//                            } catch (IOException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                    }, delay);
//                }
//
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//
//        }
        try {
            join(0);
        } catch (IOException e) {
            e.printStackTrace();
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

    /**
     * Handle master receive, once called, receives message from all processes, once received a message, put it in the
     * queue and then update the header counter.
     *
     * @param dst
     * @throws IOException
     */
    public void node_receive(int dst) throws IOException {
        System.out.println("Node Receive");
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
            System.out.println(strmsg);
            String[] msgs = strmsg.split(";");
            String real_msg = msgs[1];
            if(real_msg.startsWith("show")){

                String message = "s-show_fing ";
                String table = "";
                String keys = "";
                if(real_msg.equals("show all")){
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
                String[] strs = real_msg.split(" ");
                int key = Integer.parseInt(strs[2]);
                if(this.Local_Keys.contains(key)){
                    String message = "found "+ selfID + " in " + key;
                    unicast_send(0, message.getBytes());
                } else {

                    //Case1: Finger range includes 0.
                    if(this.Finger_table.get(7) < selfID){
                        if(key < selfID && key > this.Finger_table.get(7)){
                            String message = "find " + this.Finger_table.get(7) + " " + key;
                            unicast_send(this.Finger_table.get(7), message.getBytes());
                        } else {
                            String message = "find " + this.successor + " " + key;
                            unicast_send(this.successor, message.getBytes());
                        }
                    }
                    //Case2: Finger range doesn't include 0.
                    else {
                        if(key < selfID || key > this.Finger_table.get(7)){
                            String message = "find " + this.Finger_table.get(7) + " " + key;
                            unicast_send(this.Finger_table.get(7), message.getBytes());
                        } else {
                            String message = "find " + this.successor + " " + key;
                            unicast_send(this.successor, message.getBytes());
                        }
                    }

                }
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
                message = message.substring(0, message.length()-1);

                unicast_send(dst, message.getBytes());

            } else if(real_msg.startsWith("keys")){

                String message = "resp_keys ";
                String set = "";
                for(int i : Local_Keys){
                    set += i+"#";
                }
                message += set;
                message = message.substring(0, message.length()-1);

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
                System.out.println("Re:"+real_msg);

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

            } else if(real_msg.startsWith("rmkeys")){

                //Update finger table.
                String[] strs = real_msg.split(" ");
                int thres = Integer.parseInt(strs[1]);
                for(int i : Local_Keys){
                    if(i < thres) {
                        Local_Keys.remove(i);
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
    private void node_send(int dst, Message m) throws IOException {
        System.out.println("sending msg : " + m.msg + " to dst: " + dst);
        ObjectOutputStream oos = NhandleSendConnection(dst);
        oos.flush();
        oos.writeObject(new Message(m.Sender_ID, m.Sender_addr, m.msg));
        //TODO:what should we do here?
    }

}
