package Process;

import java.net.InetSocketAddress;
import java.util.HashMap;

class Config {
    HashMap<Integer, InetSocketAddress> m;
    int minDelay, maxDelay;

    public Config(HashMap<Integer, InetSocketAddress> m, int minDelay, int maxDelay) {
        this.m = m;
        this.maxDelay = maxDelay;
        this.minDelay = minDelay;
    }
}
