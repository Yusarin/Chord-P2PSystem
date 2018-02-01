/**
 * Created by russelluo on 2018/2/1.
 */

import org.omg.CosNaming.NamingContextPackage.NotFound;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.HashMap;

public class process {
    private int PROC_NUMBER;//this process's ID
    private Socket socket;//this process's socket
    private HashMap<Integer, Socket> groupMembers;//all other group mumbers' id

    /**
     * @param number ID assigned for this process
     * @param s socket this process bind to
     */
    public process(int number, Socket s) {
        PROC_NUMBER = number;
        socket = s;
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
     * @param id new process id
     * @param s new process id's socket
     */
    public void addNeigh(int id, Socket s) {
        groupMembers.put(id, s);
    }

    /**
     * print this process's ID, IP, port number
     */
    public void printProcessInfo() {
        System.out.printf("Process number is %d\n", this.PROC_NUMBER);
        System.out.printf("IP is: %s\n", socket.getInetAddress().toString());
        System.out.printf("Port number is: %d\n", socket.getPort());
    }

}
