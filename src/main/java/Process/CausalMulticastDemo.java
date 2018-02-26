package Process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class CausalMulticastDemo extends UnicastDemo {
    public static void main(String[] args) throws IOException {
        System.out.println("Service is up");
        BlockingQueue q = new LinkedBlockingDeque<String>(100);
        int id = Integer.parseInt(args[0]);
        Config config = UnicastDemo.parseConfig(args[1]);
        new Thread(new CausalOrderProcess(q, id, config.m, config.minDelay, config.maxDelay)).start();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String msg = br.readLine();
            q.add(msg);
        }
    }
}
