package Process;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

class Config {
    ConcurrentHashMap<Integer, InetSocketAddress> m;
    int minDelay, maxDelay;

    public Config(ConcurrentHashMap<Integer, InetSocketAddress> m, int minDelay, int maxDelay) {
        this.m = m;
        this.maxDelay = maxDelay;
        this.minDelay = minDelay;
    }
}
