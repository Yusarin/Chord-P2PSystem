package Process;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class UnicastDemo {
    public static void main(String[] args) throws IOException {
        System.out.println("please send msg");
        BlockingQueue q = new LinkedBlockingDeque<String>(100);
        int id = Integer.parseInt(args[0]);
        Config config = parseConfig(args[1]);
        new Thread(new BlockingProcess(q, id, config.m, config.minDelay, config.maxDelay)).start();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String msg = br.readLine();
            q.add(msg);
        }
    }

    protected static Config parseConfig(String filename) throws IOException {
        String reg = "\\s+";
        BufferedReader file = new BufferedReader(new FileReader(filename));
        String line = file.readLine();//first line
        if (line == null) throw new IOException("wrong formatted config file");
        String[] delays = line.split("\\s+");
        if (delays.length != 4) throw new IOException("wrong formatted config file");
        Integer min = Integer.parseInt(delays[1]);
        Integer max = Integer.parseInt(delays[3]);
        if (max == null || min == null) throw new IOException("wrong formatted config file");
        HashMap<Integer, InetSocketAddress> map = new HashMap<>();
        line = file.readLine();
        while (line != null) {
            String[] peer = line.split(reg);
            if (peer.length != 3) throw new IOException("wrong formatted config file");
            Integer port = Integer.parseInt(peer[2]);
            Integer ID = Integer.parseInt(peer[0]);
            if (ID == null || port == null) throw new IOException("wrong formatted config file");
            System.out.println(peer[1] + " : " + port);
            map.put(ID, new InetSocketAddress(peer[1], port));
            line = file.readLine();
        }
        return new Config(map, min, max);
    }
}
