import Process.BlockingProcess;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class Process {
    public static void main(String[] args) throws IOException {
        System.out.println("please send msg");
        BlockingQueue q = new LinkedBlockingDeque<String>(100);
        HashMap<Integer, InetSocketAddress> map = new HashMap<>();
        map.put(1, new InetSocketAddress("127.0.0.1", 8001));
        map.put(2, new InetSocketAddress("127.0.0.1", 8002));
        map.put(3, new InetSocketAddress("127.0.0.1", 8003));
        int id = Integer.parseInt(args[0]);
        new Thread(new BlockingProcess(q, id, map, 10, 200)).start();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String msg = br.readLine();
            q.add(msg);
        }
    }
}
