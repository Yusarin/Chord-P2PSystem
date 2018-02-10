/**
 * Created by russelluo on 2018/2/1.
 */
package Process;

import org.omg.CosNaming.NamingContextPackage.NotFound;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Timer;
import java.util.TimerTask;

enum Verb {DOWN, SEND, RECV, UNKNOWN}

class ProcessEntry {
    Verb verb;
    int size;
    int dst;
    ByteBuffer header;
    ByteBuffer dstbuf;
    ByteBuffer sizebuf;
    ByteBuffer content;

    public ProcessEntry() {
        header = ByteBuffer.allocate(4);
        sizebuf = ByteBuffer.allocate(4);
        dstbuf = ByteBuffer.allocate(4);
        dst = -1;
        content = null;
        size = -1;
        verb = Verb.UNKNOWN;
    }
}

public class Process extends Thread {
    private final int POOL_ID = 0;//default pool ID
    private final int PROC_ID;//this Process's ID
    private final InetAddress IP;//this Process's ip
    private final int port;// this Process's port
    private boolean running = false;//if the Process is running
    private ServerSocketChannel sock = ServerSocketChannel.open();
    private Selector selector = Selector.open();
    private HashMap<Integer, SocketChannel> groupMembers = new HashMap<Integer, SocketChannel>();//map Process ID's to sockets
    private HashMap<InetSocketAddress, Integer> ID_INFO = new HashMap<InetSocketAddress, Integer>();


    /**
     * @param number      ID assigned for this Process
     * @param address     IP this Process bound to
     * @param port        Port this Process bound to
     * @param poolAddress Process Pool's IP
     * @param poolPort    Process Pool's port
     */
    public Process(int number, InetAddress address, int port, InetAddress poolAddress, int poolPort) throws IOException {
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
        System.out.println("process " + PROC_ID + "is running");
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
     * Just simply plug a key in selector (and non-blocking wait for a period of time)
     * TODO: Randomize the time period
     *
     * @param dst dst Process id
     * @param msg message to send
     * @throws NotFound ID is not in neighbor list
     */
    public void unicast_send(int dst, final ProcessEntry msg) throws NotFound {
        if (groupMembers.containsKey(dst)) {
            final SocketChannel target = groupMembers.get(dst);
            new Timer().schedule(new TimerTask() {
                public void run() {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        target.register(selector, SelectionKey.OP_WRITE, msg);//attach a msg to send
                    } catch (ClosedChannelException e) {
                        e.printStackTrace();
                    }
                }
            }, 1);
        }
        throw new NotFound();
    }

    /**
     * Just simply plug a key in selector (and non-blocking wait for a period of time)
     * TODO: Randomize the time period
     *
     * @param src src Process id
     * @param msg buffer to receive message
     * @throws NotFound ID is not in neighbor list
     */
    public void unicast_receive(int src, final ProcessEntry msg) throws NotFound {
        if (groupMembers.containsKey(src)) {
            final SocketChannel target = groupMembers.get(src);
            new Timer().schedule(new TimerTask() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    try {
                        target.register(selector, SelectionKey.OP_READ, msg);//attach a msg buff to read
                    } catch (ClosedChannelException e) {
                        e.printStackTrace();
                    }
                }
            }, 1);
        }
        throw new NotFound();
    }


    /**
     * @param keyReadReady selection key ready to read
     */
    private void handleRead(SelectionKey keyReadReady) {
        SocketChannel s = (SocketChannel) keyReadReady.channel();
        InetSocketAddress incomingAddr = (InetSocketAddress) s.socket().getRemoteSocketAddress();
        ProcessEntry p = (ProcessEntry) keyReadReady.attachment();
        try {
            if (p == null) {
                p = new ProcessEntry();
            }//new entry
            if (isPool(incomingAddr)) {
                if (p.verb == Verb.UNKNOWN) {//not read verb yet
                    if (!sockReadContent(s, p.header).hasRemaining()) {
                        String v = p.header.flip().toString();
                        switch (v) {
                            case "DOWN":
                                p.verb = Verb.DOWN;
                            case "SEND":
                                p.verb = Verb.SEND;
                            case "RECV":
                                p.verb = Verb.RECV;
                        }
                    }
                } else if (p.verb == Verb.DOWN) {
                    keyReadReady.cancel();
                } else {
                    if (p.dst == -1) {//not read destination yet
                        if (!sockReadContent(s, p.dstbuf).hasRemaining()) {
                            p.dst = p.dstbuf.flip().getInt();
                        }
                    } else {
                        if (p.size == -1) {//not read size yet
                            if (!sockReadContent(s, p.sizebuf).hasRemaining()) {
                                p.size = p.sizebuf.flip().getInt();
                                p.content = ByteBuffer.allocate(p.size);
                            }
                        } else {
                            if (p.verb == Verb.RECV) {
                                unicast_receive(p.dst, p);
                                keyReadReady.cancel();
                            } else if (p.verb == Verb.SEND) {
                                if (!sockReadContent(s, p.content).hasRemaining()) {
                                    p.content.flip();
                                    unicast_send(p.dst, p);
                                    keyReadReady.cancel();
                                }
                            }
                        }
                    }
                }
            } else {
                if (!sockReadContent(s, p.content).hasRemaining()) {
                    System.out.println("Read a msg" + p.content.toString());
                    keyReadReady.cancel();
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Loss connection!");
        } catch (NotFound notFound) {
            notFound.printStackTrace();
            System.out.println("This is not a legal process number!");
        }
        //TODO:implement this handleRead function
    }

    /**
     * @param keyWriteReady selection key ready to write
     */
    private void handleWrite(SelectionKey keyWriteReady) {
        SocketChannel s = (SocketChannel) keyWriteReady.channel();
        InetSocketAddress incomingAddr = (InetSocketAddress) s.socket().getRemoteSocketAddress();
        ProcessEntry p = (ProcessEntry) keyWriteReady.attachment();
        if (isPool(incomingAddr)) {
            //TODO implement pool msg write
            if (!sockWriteContent().hasRemaining()) {

            }
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
            s.configureBlocking(false);//set to unblocking-mode
            InetSocketAddress addr = (InetSocketAddress) s.socket().getRemoteSocketAddress();//remote address
            int id = ID_INFO.get(addr);//find ID of in-coming connection
            groupMembers.put(id, s);
            s.register(selector, SelectionKey.OP_READ | SelectionKey.OP_WRITE);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println("New Process was added");
    }

    /**
     * print this Process's ID, IP, port number
     */
    public void printProcessInfo() {
        System.out.printf("Process number is %d\n", this.PROC_ID);
        System.out.printf("IP is: %s\n", IP.toString());
        System.out.printf("Port number is: %d\n", port);
    }

    /**
     * @return whether the Process is OK to connect
     */
    public boolean isRunning() {
        return running;
    }

    private boolean isPool(InetSocketAddress addr) {
        return ID_INFO.get(addr) == 0;
    }

    //Add socket to the groupMember of this Process. Called by pool's method.
    public void AddtoGroup(int pid, InetSocketAddress socketAddress) throws IOException {
        if (this.groupMembers.containsKey(pid)) return;
        this.groupMembers.put(pid, SocketChannel.open(socketAddress));
        this.ID_INFO.put(socketAddress, pid);
    }


    public ByteBuffer sockReadContent(SocketChannel sock, ByteBuffer buf) throws IOException {
        if (sock.read(buf) == -1) {
            throw new IOException();
        }
        return buf;
    }

    public ByteBuffer sockWriteContent(SocketChannel sock, ByteBuffer buf) throws IOException {
        if (sock.write(buf) == -1) {
            throw new IOException();
        }
        return buf;
    }
}

