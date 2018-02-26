package Process;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.Comparator;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.PriorityBlockingQueue;

public class CausalOrderProcess extends BlockingProcess {
    private final VectorClock clock;
    private final int clockPos;
    private final BlockingQueue deliverQueue = new PriorityBlockingQueue(100);

    public CausalOrderProcess(BlockingQueue q, int ID, HashMap<Integer, InetSocketAddress> map, int min_delay, int max_delay) throws IOException {
        super(q, ID, map, min_delay, max_delay);
        clock = new VectorClock(new int[map.size()]);// initialize to (0,0,0,0...)
        clockPos = ID - 1;
    }

    public void CausalSend(int ID, Byte[] msg) {
        clock.incAt(clockPos);
    }

    public void CausalRecv(int ID, Byte[] msg) {
    }


}
