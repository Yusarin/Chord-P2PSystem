package Process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class CausalMulticastDemo extends UnicastDemo {
    public static void main(String[] args) throws IOException {
        System.out.println("Service is up");
        BlockingQueue q = new LinkedBlockingDeque<String>(100);
        try {
            int id = Integer.parseInt(args[0]);
            Config config = UnicastDemo.parseConfig(args[1]);
            if (id != 0)
                new Thread(new CausalOrderProcess(q, id, config.m, config.minDelay, config.maxDelay)).start();
            BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
            if (args.length > 2) {
                System.out.println("reading script...");
                ArrayList<String> lines = readScript(args[2]);
                for (String line : lines) {
                    q.add(line);
                }
            }
            while (true) {
                String msg = br.readLine();
                q.add(msg);
            }
        } catch (IndexOutOfBoundsException e) {
            System.out.println("Please include ID, config file, and optional script");
        }
    }
}
