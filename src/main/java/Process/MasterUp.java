package Process;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

public class MasterUp extends Process {
    public static void main(String[] args) throws IOException {
        System.out.println("Launching Sequencer......");
        BlockingQueue q = new LinkedBlockingDeque<String>(100);
        Config config = Process.parseConfig(args[1]);
        //if(id == 1)
        //    new Thread(new Master(q, 0, config.m, config.minDelay, config.maxDelay)).start();
        new Thread(new Master(q, 0, config.m, config.minDelay, config.maxDelay)).start();
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            String msg = br.readLine();
            q.add(msg);
        }
    }
}
