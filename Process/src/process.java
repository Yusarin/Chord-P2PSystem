/**
 * Created by russelluo on 2018/2/1.
 */

import org.omg.CosNaming.NamingContextPackage.NotFound;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.*;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.util.HashMap;

public class process extends Thread {
    private final int POOL_ID = 0;//default pool ID
    private final int PROC_ID;//this process's ID
    private final InetAddress IP;//this process's ip
    private final int port;// this process's port
    private boolean running=false;
    private AsynchronousServerSocketChannel sock;
    private HashMap<Integer, Socket> groupMembers;//map process ID's to sockets

    /**
     * @param number ID assigned for this process
     * @param s      IP this process bound to
     * @param poolPort Process Pool's port
     * @param port Port this process bound to
     */
    public process(int number, InetAddress s, int port, int poolPort) throws IOException {
        super();
        PROC_ID = number;
        IP = s;
        this.port = port;
        sock = AsynchronousServerSocketChannel.open().bind(new InetSocketAddress(IP,port));
        groupMembers = new HashMap<Integer, Socket>();
        groupMembers.put(POOL_ID, new Socket(IP, poolPort));
    }

    @Override
    public void run() {
        running = true;
        sock.accept();
        //TODO:implement this run method
    }

    /**
     * @param dst dst process id
     * @param msg message to send
     * @throws NotFound ID is not in neighbor list
     */
    public void unicast_send(int dst, byte[] msg) throws NotFound {
        if (groupMembers.containsKey(dst)) {
            Socket target = groupMembers.get(dst);
            try {
                OutputStream out = target.getOutputStream();
                out.write(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new NotFound();
    }

    /**
     * @param src src process id
     * @param msg buffer to receive message
     * @throws NotFound ID is not in neighbor list
     */
    public void unicast_receive(int src, byte[] msg) throws NotFound {
        if (groupMembers.containsKey(src)) {
            Socket target = groupMembers.get(src);
            try {
                InputStream in = target.getInputStream();
                int len = msg.length;
                in.read(msg);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new NotFound();
    }

    /**
     * @param id   new process id
     * @param ip   new process's ip
     * @param port new process's port
     */
    public void addNeigh(int id, InetAddress ip, int port) throws IOException {
        Socket s = new Socket(ip, port);
        groupMembers.put(id, s);
    }

    /**
     * print this process's ID, IP, port number
     */
    public void printProcessInfo() {
        System.out.printf("Process number is %d\n", this.PROC_ID);
        System.out.printf("IP is: %s\n", IP.toString());
        System.out.printf("Port number is: %d\n", port);
    }

}
