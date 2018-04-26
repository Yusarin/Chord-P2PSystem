package Process;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

public class BlockingProcess implements Runnable {
    protected final BlockingQueue writeQueue;
    protected final Map<Integer, ObjectOutputStream> idMapOOS = new ConcurrentHashMap<>();//map id to socket
    protected final Map<Integer, Socket> idMapSocket = new ConcurrentHashMap<>();//map id to socket
    protected Map<InetSocketAddress, Integer> ipMapId;//map ip to id
    protected final Map<Integer, InetSocketAddress> idMapIp;//map id to ip
    protected final Map<Integer, InetSocketAddress> running;
    protected final int selfID;
    protected InetSocketAddress addr;
    protected ServerSocket sock;
    protected final int min_delay;
    protected final int max_delay;
    protected Lock writeLock = new ReentrantLock();
    public HashMap<Integer, Integer> Finger_table;
    public HashSet<Integer> Local_Keys;
    int successor;
    int predecessor;
    String wait_succ = "wait";
    String wait_pred = "wait";
    String wait_fin = "wait";
    String wait_keys = "wait";


    public BlockingProcess(BlockingQueue q, int selfID, ConcurrentHashMap<Integer, InetSocketAddress> map,
                           int min_delay, int max_delay) throws IOException {
        this.addr = map.get(selfID);
        sock = new ServerSocket();
        sock.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        sock.setOption(StandardSocketOptions.SO_REUSEPORT, true);
        //sock.bind(this.addr);
        this.selfID = selfID;
        this.writeQueue = q;
        this.max_delay = max_delay;
        this.min_delay = min_delay;
        idMapIp = new HashMap<>();
        this.running = map;
        Finger_table = new HashMap<>();
        Local_Keys = new HashSet<>();
        ipMapId = reverseMap(idMapIp);
        successor = 0;
        predecessor = 0;
    }

    /**
     * This thread will spawn a thread to listen to a socket if a new connection is accepted.
     */
    protected void startAcceptingThread() {
        new Thread(() -> {
            while (true) {
                try {
                    Socket s = sock.accept();
                    Integer newID;
                    if (!idMapOOS.containsValue(s)) {
                        newID = ipMapId.get(s.getRemoteSocketAddress());
                        System.out.println("incoming id: " + newID);
                        assert newID != null;
                        idMapOOS.put(newID, new ObjectOutputStream(s.getOutputStream()));
                        idMapSocket.put(newID, s);
                        new Thread(() -> {
                            try {
                                unicast_receive(newID);
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }).start();
                    } else {
                        throw new ConnectException("Already accept");
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }).start();
        System.out.println("accepting thread up");
    }

    /**
     * This thread spawn three new thread
     * 1.Accept new connection
     * 2.Deliver packet
     * 3.Send packet
     */
    @Override
    public void run() {
        System.out.println("server is up");
        System.out.println("listening on " + sock);
        startAcceptingThread();
        while (true) {
            try {
                final String msg = (String) writeQueue.take();
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                System.out.println("delay is: " + delay);
                String parsed[] = msg.split("\\s+", 3);
                if (parsed.length != 3)
                    throw new IllegalArgumentException();
                if (parsed[0].equals("send")) {
                    if (idMapIp.containsKey(Integer.parseInt(parsed[1]))) {
                        new Timer().schedule(new TimerTask() {
                            @Override
                            public void run() {
                                try {
                                    unicast_send(Integer.parseInt(parsed[1]), parsed[2].getBytes());
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }
                        }, delay);
                    }
                } else {
                    throw new IllegalArgumentException();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (IllegalArgumentException e) {
                System.out.println("not a legal command");
            }
        }
    }

    /**
     * This function handle connection (client side). If this is the first message, the new established
     * Socket need to be added to global maps. Otherwise, it just pull out the record from the map.
     *
     * @param dst
     * @return
     * @throws IOException
     */
    protected final ObjectOutputStream handleSendConnection(int dst) throws IOException {
        Socket s;
        ObjectOutputStream oos = null;
        if (idMapOOS.containsKey(dst)) {
            oos = idMapOOS.get(dst);
        } else {//this is first time connection
            s = new Socket();
            s.setOption(StandardSocketOptions.SO_REUSEPORT, true);
            s.bind(addr);
            InetSocketAddress id;
            id = idMapIp.get(dst);
            System.out.println(id);
            s.connect(id);
            oos = new ObjectOutputStream(s.getOutputStream());
            idMapOOS.put(dst, oos);
            idMapSocket.put(dst, s);
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
     * Handle unicast send. There is not much to say here.
     *
     * @param dst
     * @param msg
     * @throws IOException
     */
    protected void unicast_send(int dst, byte[] msg) throws IOException {
        System.out.println("sending msg : " + new String(msg) + " to dst: " + dst);
        ObjectOutputStream oos;
        oos = handleSendConnection(dst);
        Message message = new Message(selfID, addr, new String(msg));
        if (dst == selfID) {
            System.out.println("You are sending message to yourself");
            return;
        }
        writeLock.lock();
        oos.flush();// TODO:Do we need flush?
        oos.writeObject(message);
        writeLock.unlock();
    }

    /**
     * Handle receive. Once see a packet, put the packet in the queue
     *
     * @param dst
     * @throws IOException
     */
    protected void unicast_receive(int dst) throws IOException {
        System.out.println("Unicast Receive");
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
    //Ask Node n to find successor of id.
    public int find_successor(int n, int id) throws IOException{
        int np = find_predecessor(n, id);
        return askforsucc(np);
    }


    //Ask Node n to find predecessor of id.
    public int find_predecessor(int n, int id) throws IOException{
        int np = n;
        while(id <= np || id > askforsucc(np)){
            System.out.println("In find predecessor while");
            np = closest_preceding_finger(np, id);
        }
        return np;
    }

    public int closest_preceding_finger(int n, int id) throws IOException{
        HashMap<Integer, Integer> remotetable = new HashMap<>();
        remotetable = askforfingertable(n);
        for(int i = 7 ; i >= 0 ; i--){

            int node_num = remotetable.get(i);
            System.out.println(i+" is good");
            if(node_num > n && node_num < id){
                return node_num;
            }
        }
        return n;
    }


    public int askforsucc(int id) throws IOException{
        String message = "succ";
        unicast_send(id, message.getBytes());
        while(wait_succ.equals("wait")){
            do_job();
        };
        System.out.println("succ changed");
        int res = Integer.parseInt(wait_succ);
        wait_succ = "wait";
        return res;
    }

    public int askforpred(int id) throws IOException{
        String message = "pred";
        unicast_send(id, message.getBytes());
        while(wait_pred.equals("wait")){
            do_job();
        };
        System.out.println("pred changed");

        int res = Integer.parseInt(wait_pred);
        wait_pred = "wait";
        return res;
    }

    public HashMap askforfingertable(int id) throws IOException{
        String message = "fing";
        unicast_send(id, message.getBytes());
        while(wait_fin.equals("wait")){
            do_job();
        };
        System.out.println("fin changed");
        System.out.println(wait_fin);

        String[] entries = wait_fin.split("#");
        HashMap<Integer, Integer> map = new HashMap<>();
        for(int i = 0 ; i < entries.length ; i++){
            String[] each = entries[i].split(",");
            if(each.length > 1)
                map.put(Integer.parseInt(each[0]), Integer.parseInt(each[1]));
        }
        System.out.println("map size:"+map.size());
        wait_fin = "wait";
        return map;
    }

    public String[] askforkeys(int id) throws IOException{
        String message = "keys";
        unicast_send(id, message.getBytes());
        while(wait_keys.equals("wait")){
            do_job();
        };
        System.out.println("keys changed");

        String[] res = wait_keys.split("#");
        return res;
    }

    public void setPred(int id, int val) throws IOException{
        String message = "setPred "+val;
        unicast_send(id, message.getBytes());
    }

    public void setSucc(int id, int val) throws IOException{
        String message = "setSucc "+val;
        unicast_send(id, message.getBytes());
    }


    public void join(int np) throws IOException{
        init_finger_table(np);
        update_others();
        //TODO:Move keys.
        String[] keys_succ = askforkeys(this.successor);
        for(int i = 0 ; i < keys_succ.length ; i++){
            if(Integer.parseInt(keys_succ[i]) <= selfID){
                this.Local_Keys.add(Integer.parseInt(keys_succ[i]));
            }
        }
        String message = "rmkeys "+ selfID;
        unicast_send(this.successor, message.getBytes());
    }

    //Initialize local finger table with the help of np.
    public void init_finger_table(int np) throws IOException{
        int ini = find_successor(np, getStart(selfID, 0));
        Finger_table.put(0, ini);
        this.predecessor = askforpred(this.successor);
        setPred(this.successor, selfID);


        for(int i = 0; i < 7 ; i++){
            int start = getStart(selfID, i+1);
            if(start >= selfID && start < Finger_table.get(i)){
                Finger_table.put(i+1, Finger_table.get(i));
            } else{
                int val = find_successor(np, start);
                Finger_table.put(i+1, val);
            }
        }
    }

    public void update_others() throws IOException{
        for(int i = 0 ; i < 8 ; i++){
            int p = find_predecessor(selfID, selfID - (int)Math.pow(2,i));
            String msg = "update_finger_table "+selfID+" "+i;
            unicast_send(p, msg.getBytes());
        }
    }

    public void update_finger_table(int s, int i) throws IOException{
        if(s >= selfID && s < Finger_table.get(i)){
            Finger_table.put(i, s);
            int p = this.predecessor;
            String msg = "update_finger_table "+s+" "+i;
            unicast_send(p, msg.getBytes());
        }
    }

    /**
     * A helper function to "reverse" a map.
     *
     * @param map
     * @return
     */
    protected static Map<InetSocketAddress, Integer> reverseMap(Map<Integer, InetSocketAddress> map) {
        Map<InetSocketAddress, Integer> map_r = new ConcurrentHashMap<>();
        for (Map.Entry<Integer, InetSocketAddress> e : map.entrySet()) {
            map_r.put(e.getValue(), e.getKey());
        }
        return map_r;
    }

    public int getStart(int n, int k){
        int total = n + (int)Math.pow(2, k);
        return total % 256;
    }


    public void do_job(){
        long i = 0;
        long j = 9999;
        while(j > 0) {
            while (i < 42500000) {
                i += 1;
                j--;
            }
        }
    }
}
