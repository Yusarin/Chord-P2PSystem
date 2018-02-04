/**
 * Created by russelluo on 2018/2/1.
 */

import org.omg.CosNaming.NamingContextPackage.NotFound;

import java.io.IOException;
import java.net.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Iterator;

public class process extends Thread {
    private final int POOL_ID = 0;//default pool ID
    private final int PROC_ID;//this process's ID
    private final InetAddress IP;//this process's ip
    private final int port;// this process's port
    private boolean running = false;//if the process is running
    private ServerSocketChannel sock = ServerSocketChannel.open();
    private Selector selector = Selector.open();
    private HashMap<Integer, SocketChannel> groupMembers = new HashMap<Integer, SocketChannel>();//map process ID's to sockets
    private HashMap<InetSocketAddress, Integer> ID_INFO = new HashMap<InetSocketAddress, Integer>();

    /**
     * @param number   ID assigned for this process
     * @param address  IP this process bound to
     * @param poolPort Process Pool's port
     * @param port     Port this process bound to
     */
    public process(int number, InetAddress address, int port, InetAddress poolAddress, int poolPort) throws IOException {
        super();
        PROC_ID = number;
        IP = address;
        this.port = port;
        InetSocketAddress poolSockAddr = new InetSocketAddress(poolAddress, poolPort);
        SocketChannel poolSock = SocketChannel.open(poolSockAddr);
        groupMembers.put(POOL_ID, poolSock);
        ID_INFO.put(poolSockAddr, POOL_ID);
        sock.socket().bind(new InetSocketAddress(IP, port));
        sock.configureBlocking(false);//set the socket to non-blocking socket.
        sock.register(selector, SelectionKey.OP_ACCEPT);//only accept server socket
        groupMembers.get(POOL_ID).register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);//registor pool_socket into selector
    }

    /**
     * Override a run method, implement a NIO server event loop
     */
    @Override
    public void run() {
        running = true;
        System.out.println("Process " + PROC_ID + "is running");
        Iterator<SelectionKey> iter;
        SelectionKey key;
        try {
            while (sock.isOpen()) {
                selector.select();
                iter = selector.selectedKeys().iterator();
                while (iter.hasNext()) {
                    key = iter.next();
                    iter.remove();
                    if (key.isAcceptable()) {
                        handleAccept(key);
                    }
                    if (key.isReadable()) {
                        handleRead(key);
                    }
                    if (key.isWritable()) {
                        handleWrite(key);
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("IOError");
            e.printStackTrace();
        }
    }

    /**
     * Just simply plug a key in selector
     *
     * @param dst dst process id
     * @param msg message to send
     * @throws NotFound ID is not in neighbor list
     */
    public void unicast_send(int dst, byte[] msg) throws NotFound {
        if (groupMembers.containsKey(dst)) {
            SocketChannel target = groupMembers.get(dst);
            try {
                target.register(selector, SelectionKey.OP_WRITE, msg);//attach a msg to send
            } catch (ClosedChannelException e) {
                e.printStackTrace();
            }
        }
        throw new NotFound();
    }

    /**
     * Just simply plug a key in selector
     *
     * @param src src process id
     * @param msg buffer to receive message
     * @throws NotFound ID is not in neighbor list
     */
    public void unicast_receive(int src, byte[] msg) throws NotFound {
        if (groupMembers.containsKey(src)) {
            SocketChannel target = groupMembers.get(src);
            try {
                target.register(selector, SelectionKey.OP_READ, msg);//attach a msg buff to read
            } catch (ClosedChannelException e) {
                e.printStackTrace();
            }
        }
        throw new NotFound();
    }


    /**
     * @param keyReadReady selection key ready to read
     */
    private void handleRead(SelectionKey keyReadReady) {
        SocketChannel s = (SocketChannel) keyReadReady.channel();
        byte[] msg = (byte[]) keyReadReady.attachment();
        InetSocketAddress incomingAddr = (InetSocketAddress) s.socket().getRemoteSocketAddress();
        ByteBuffer buf = ByteBuffer.allocate(msg.length);
        if (isPool(incomingAddr)) {
            //TODO implement pool msg read
        } else {
            //TODO:implement normal read
        }
        buf.wrap(msg);
        System.out.println("Read a msg");
        //TODO:implement this handleRead function
    }

    /**
     * @param keyWriteReady selection key ready to write
     */
    private void handleWrite(SelectionKey keyWriteReady) {
        SocketChannel s = (SocketChannel) keyWriteReady.channel();
        byte[] msg = (byte[]) keyWriteReady.attachment();
        InetSocketAddress incomingAddr = (InetSocketAddress) s.socket().getRemoteSocketAddress();
        ByteBuffer buf = ByteBuffer.wrap(msg);
        if (isPool(incomingAddr)) {
            //TODO implement pool msg write
        } else {
            //TODO implement normal msg write
        }
        System.out.println("write a msg");
        //TODO:implement this handleWrite function
    }

    /**
     * @param keyAcceptReady selection key ready to accept
     */
    private void handleAccept(SelectionKey keyAcceptReady) {
        try {
            SocketChannel s = ((ServerSocketChannel) keyAcceptReady.channel()).accept();//incoming connection socket
            InetSocketAddress addr = (InetSocketAddress) s.socket().getRemoteSocketAddress();//remote address
            int id = ID_INFO.get(addr);//find ID of in-coming connection
            groupMembers.put(id, s);
            s.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("New process was added");
    }

    /**
     * print this process's ID, IP, port number
     */
    public void printProcessInfo() {
        System.out.printf("Process number is %d\n", this.PROC_ID);
        System.out.printf("IP is: %s\n", IP.toString());
        System.out.printf("Port number is: %d\n", port);
    }

    /**
     * @return whether the process is OK to connect
     */
    public boolean isRunning() {
        return running;
    }

    private boolean isPool(InetSocketAddress addr) {
        return ID_INFO.get(addr) == 0;
    }
    
    //Add socket to the groupMember of this process. Called by pool's method.
    public void AddtoGroup(int pid, InetSocketAddress socketAddress){
        if(this.groupMembers.containsKey(pid)) return;
        this.groupMembers.put(pid, SocketChannel.open(socketAddress));
        this.ID_INFO.put(socketAddress, pid);
    }
}
