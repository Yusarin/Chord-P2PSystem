package Process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class TotalOrderDemo extends UnicastDemo {
    public static void main(String[] args) throws IOException {
        System.out.println("Total order demo Service is up");
        BlockingQueue q = new LinkedBlockingDeque<String>(100);
        int id = Integer.parseInt(args[0]);
        Config config = UnicastDemo.parseConfig(args[1]);
        //if(id == 1)
        //    new Thread(new Master(q, 0, config.m, config.minDelay, config.maxDelay)).start();
        new Thread(new TotalOrderProcess(q, id, config.m, config.minDelay, config.maxDelay)).start();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        if (args.length > 2) {
            System.out.println("reading script...");
            ArrayList<String> lines = readScript(args[2]);
            if (lines != null)
                for (String line : lines) {
                    q.add(line);
                }
        }
        while (true) {
            String msg = br.readLine();
            q.add(msg);
        }
    }
}
