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
    protected Map<Integer, InetSocketAddress> running = new ConcurrentHashMap<>();;
    protected final int selfID;
    protected InetSocketAddress addr;
    protected ServerSocket sock;
    protected final int min_delay;
    protected final int max_delay;
    protected Lock writeLock = new ReentrantLock();
    public HashMap<Integer, Integer> Finger_table;
    public HashSet<Integer> Local_Keys;
    public int Heartbeat_Period;
    boolean Alive;
    int successor;
    int predecessor;
    String wait_succ = "wait";
    String wait_pred = "wait";
    String wait_fin = "wait";
    String wait_keys = "wait";
    int num_send;


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
        this.num_send = 0;
        this.Heartbeat_Period = 1;
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
    }

    /**
     * This thread spawn three new thread
     * 1.Accept new connection
     * 2.Deliver packet
     * 3.Send packet
     */
    @Override
    public void run() {

        startAcceptingThread();
        while (true) {
            try {
                final String msg = (String) writeQueue.take();
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
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
        //System.out.println("sending msg : " + new String(msg) + " to dst: " + dst);
        final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                try {
                    num_send++;
                    if (dst == selfID) {
                        System.out.println("You are sending message to yourself" + selfID);
                        return;
                    }
                    ObjectOutputStream oos;
                    oos = handleSendConnection(dst);
                    Message message = new Message(selfID, addr, new String(msg));
                    writeLock.lock();
                    oos.flush();// TODO:Do we need flush?
                    oos.writeObject(message);
                    writeLock.unlock();
                }
             catch (IOException e){
                e.printStackTrace();
            }
        }
        }, delay);

    }

    /**
     * Handle receive. Once see a packet, put the packet in the queue
     *
     * @param dst
     * @throws IOException
     */
    protected void unicast_receive(int dst) throws IOException {
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
                if(table.length() > 1)
                    table = table.substring(0,table.length()-1);

                for(int i : Local_Keys){
                    keys += i+",";
                }
                if(keys.length() > 1)
                    keys = keys.substring(0, keys.length()-1);

                message += table + " ";
                message += keys;
                unicast_send(0, message.getBytes());

            } else if(real_msg.startsWith("crashed")){
                //TODO: implement crash.
                String[] strs = real_msg.split(" ");
                int c = Integer.parseInt(strs[1]);
                if(this.successor == c){
                    this.successor = find_successor(0, c);
                }
                if(this.predecessor == c){
                    this.predecessor = find_predecessor(0, c);
                    for(int i = this.predecessor+1 ; i <= c ; i++){
                        this.Local_Keys.add(i);
                    }
                }

                for(int i = 0 ; i < 8 ; i++){
                    if(Finger_table.get(i) == c){
                        int sub = find_successor(0, c);
                        Finger_table.put(i, sub);
                    }
                }
            } else if(real_msg.startsWith("find")){
                //TODO: implement find.
                String[] strs = real_msg.split(" ");
                int key = Integer.parseInt(strs[2]);
                if(this.Local_Keys.contains(key)){
                    String message = "found "+ key + " in " + selfID;
                    System.out.println(message);
                } else {
                    //Case1: Finger range includes 0.
                    if(this.Finger_table.get(7) < selfID){
                        if((key < selfID && key > this.Finger_table.get(7)) && selfID != this.Finger_table.get(7)){
                            String message = "find " + this.Finger_table.get(7) + " " + key;
                            unicast_send(this.Finger_table.get(7), message.getBytes());
                        } else {
                            String message = "find " + this.successor + " " + key;
                            unicast_send(this.successor, message.getBytes());
                        }
                    }
                    //Case2: Finger range doesn't include 0.
                    else {
                        if((key < selfID || key > this.Finger_table.get(7)) && selfID != this.Finger_table.get(7)){
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
                for(int i = 0 ; getStart(selfID, i) < this.successor && i < 8 ; i++){
                    this.Finger_table.put(i,this.successor);
                }

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
                if(strs.length > 1)
                    wait_keys = strs[1];
                else
                    wait_keys = "";

            } else if(real_msg.startsWith("rmkeys")){

                //Update keys
                String[] strs = real_msg.split(" ");
                int thres = Integer.parseInt(strs[1]);
                List<Integer> KeyList = new ArrayList<>();
                KeyList.addAll(Local_Keys);
                for(int i : KeyList){
                    if(i <= thres) {
                        Local_Keys.remove(i);
                    }
                }
                System.out.println("Keys updated");
            } else if(real_msg.startsWith("SeeNum")) {
                String msg = "Node "+selfID + " sends "+ this.num_send + " messages.";
                this.num_send = 0;
                System.out.println(msg);
            }
        }
    }
    //Ask Node n to find successor of id.
    public int find_successor(int n, int id) throws IOException{
        if(id > 256) id -= 256;
        if(n == 0) {
            List<Integer> runn = new ArrayList<>();
            runn.add(0);
            for(int i : this.running.keySet()){
                runn.add(i);
            }
            Collections.sort(runn);
            if(runn.get(runn.size()-1) <= id) return 0;
            for(int j : runn){
                if(j > id) return j;
            }
        }
        int np = find_predecessor(n, id);
        return askforsucc(np);
    }


    //Ask Node n to find predecessor of id.
    public int find_predecessor(int n, int id) throws IOException{
        if(id > 256) id -= 256;
        if(n == 0) {
            List<Integer> runn = new ArrayList<>();
            runn.add(0);
            for(int i : this.running.keySet()){
                runn.add(i);
            }
            Collections.sort(runn);
            Collections.reverse(runn);
            if(runn.get(0) > id) return 0;
            for(int j : runn){
                if(j < id) return j;
            }
        }
        int np = n;
        int npsucc = askforsucc(np);
        boolean cond;
        if(np < npsucc){
            cond = id <= np || id > npsucc;
        } else {
            cond = id <= np && id > npsucc;
        }
        while(cond){

            np = closest_preceding_finger(np, id);
            npsucc = askforsucc(np);
            if(np < npsucc){
                cond = id <= np || id > npsucc;
            } else {
                cond = id <= np && id > npsucc;
            }
//            if(tmp == np) break;
//            else np = tmp;
        }
        return np;
    }

    public int closest_preceding_finger(int n, int id) throws IOException{
        HashMap<Integer, Integer> remotetable = new HashMap<>();
        remotetable = askforfingertable(n);
        for(int i = 7 ; i >= 0 ; i--){

            int node_num = remotetable.get(i);
            boolean cond;
            if(n < id)
                cond = node_num > n && node_num < id;
            else
                cond = node_num > n || node_num < id;
            if(cond){
                return node_num;
            }
        }
        return n;
    }


    public int askforsucc(int id) throws IOException{
        if(id == selfID) return this.successor;
        String message = "succ";
        unicast_send(id, message.getBytes());
        while(wait_succ.equals("wait")){
            do_job();
        };
        int res = Integer.parseInt(wait_succ);
        wait_succ = "wait";
        return res;
    }

    public int askforpred(int id) throws IOException{
        if(id == selfID) return this.predecessor;
        String message = "pred";
        unicast_send(id, message.getBytes());
        while(wait_pred.equals("wait")){
            do_job();
        };

        int res = Integer.parseInt(wait_pred);
        wait_pred = "wait";
        return res;
    }

    public HashMap askforfingertable(int id) throws IOException{
        if(id == selfID) return this.Finger_table;
        String message = "fing";
        unicast_send(id, message.getBytes());
        while(wait_fin.equals("wait")){
            do_job();
        };
        String[] entries = wait_fin.split("#");
        HashMap<Integer, Integer> map = new HashMap<>();
        for(int i = 0 ; i < entries.length ; i++){
            String[] each = entries[i].split(",");
            if(each.length > 1)
                map.put(Integer.parseInt(each[0]), Integer.parseInt(each[1]));
        }
        wait_fin = "wait";
        return map;
    }

    public String[] askforkeys(int id) throws IOException{
        if(id == selfID){
            String[] res= new String[Local_Keys.size()];
            int cnt = 0;
            for(int i : Local_Keys){
                res[cnt] = ""+i;
                cnt += 1;
            }
            return res;
        }
        String message = "keys";
        unicast_send(id, message.getBytes());
        while(wait_keys.equals("wait")){
            do_job();
        };
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
            int k = Integer.parseInt(keys_succ[i]);
            if(k <= selfID){
                this.Local_Keys.add(Integer.parseInt(keys_succ[i]));
            }
        }
        if(selfID != 0) Local_Keys.remove(0);
        String message = "rmkeys "+ selfID;
        unicast_send(this.successor, message.getBytes());
        System.out.println("join finished");
    }

    //Initialize local finger table with the help of np.
    public void init_finger_table(int np) throws IOException{
        int ini = find_successor(np, getStart(selfID, 0));
        Finger_table.put(0, ini);
        this.successor = ini;
        this.predecessor = find_predecessor(np, selfID);
        setPred(this.successor, selfID);
        setSucc(this.predecessor, selfID);


        for(int i = 0; i < 7 ; i++){
            int start = getStart(selfID, i+1);
            boolean cond;
            if(selfID < Finger_table.get(i)) {
                cond = start >= selfID && start <= Finger_table.get(i);
            }
            else  {
                cond = start >= selfID || start <= Finger_table.get(i);
            }

            if(cond){
                Finger_table.put(i+1, Finger_table.get(i));
            } else{
                int val = find_successor(0, start);
                Finger_table.put(i+1, val);
            }
        }
    }

    public void update_others() throws IOException{
        for(int i = 0 ; i < 8 ; i++){
            int id = selfID - (int)Math.pow(2,i);
            if(id < 0) {
                id += 256;
            }
            int p;
            if(id == this.predecessor)
                p = this.predecessor;
            else
                p = find_predecessor(selfID, id);

            String msg = "update_finger_table "+selfID+" "+i;
            unicast_send(p, msg.getBytes());
        }
    }

    public void update_finger_table(int s, int i) throws IOException{
        boolean cond;
        if(selfID < Finger_table.get(i))
            cond = s > selfID && s < Finger_table.get(i);
        else
            cond = s > selfID || s < Finger_table.get(i);

        if(cond){
            Finger_table.put(i, s);
            if(i == 0 && s != selfID) {
                this.successor = s;
            }
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
