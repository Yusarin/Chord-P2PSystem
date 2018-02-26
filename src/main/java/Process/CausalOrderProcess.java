package Process;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;

public class CausalOrderProcess extends BlockingProcess {
    private final VectorClock clock;
    private final int clockPos;
    private final BlockingQueue deliverQueue = new PriorityBlockingQueue(100);

    public CausalOrderProcess(BlockingQueue q, int ID, HashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        super(q, ID, map, min_delay, max_delay);
        clock = new VectorClock(new int[map.size()]);// initialize to (0,0,0,0...)
        clockPos = ID - 1;
        System.out.println("Current vector clock:" + clock);
    }

    @Override
    public void run() {
        startAcceptingThread();
        new Thread(new DeliverThread(deliverQueue, null)).start();// start a DeliverThread
        while (true) {
            try {
                final String msg = (String) writeQueue.poll(1, TimeUnit.DAYS);
                final long delay = (long) (new Random().nextDouble() * (max_delay - min_delay)) + min_delay;
                new Timer().schedule(new TimerTask() {
                    @Override
                    public void run() {
                        try {
                            System.out.println("delay is :" + delay);
                            String parsed[] = msg.split(" ", 3);
                            if (parsed.length != 3) {
                                System.out.println("not a legal command");
                                return;
                            }
                            if (parsed[0].equals("msend")) {
                                if (idMapIp.containsKey(Integer.parseInt(parsed[1]))) {
                                    causalSend(Integer.parseInt(parsed[1]), parsed[2].getBytes());
                                }//TODO:Only msend?
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

    protected void causalSend(int ID, byte[] msg) throws IOException {
        clock.incAt(clockPos);
        System.out.println("Current vector clock:" + clock);
    }//TODO: need to finish causalSend

}
