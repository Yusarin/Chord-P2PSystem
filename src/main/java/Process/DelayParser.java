package Process;

import java.util.HashMap;
import java.util.Map;

public class DelayParser {
    private String delays;
    private int totalProcessNum;
    private int selfID;

    public DelayParser(String delays, int totalProcessNum, int selfID) {
        this.delays = delays;
        this.totalProcessNum = totalProcessNum;
        this.selfID = selfID;
    }

    /**
     * parse the string and return a map to determine send packet delay.
     * The last is default value for other processes
     * <p>
     * Format: a(id)=b(delay),c(id)=d(delay)....
     *
     * @return a map from processID to delay
     */
    public HashMap<Integer, Long> parse() throws NumberFormatException {
        String[] parsed = delays.trim().split("\\s*,\\s*", -1);
        HashMap<Integer, Long> result = new HashMap<>();
        long lastDelay = 0;
        for (String i : parsed) {
            String[] id_delay = i.split("\\s*=\\s*");
            try {
                int id = Integer.parseInt(id_delay[0]);
                long delay = Long.parseLong(id_delay[1]);
                lastDelay = delay;
                result.put(id, delay);
            } catch (NumberFormatException e) {
                throw new NumberFormatException();
            }
        }
        for (int id = 1; id <= totalProcessNum; ++id) {
            if (!result.containsKey(id)) {
                result.put(id, lastDelay);
            }
        }
        result.put(selfID, (long) 0);
        return result;
    }

    /**
     * Simple test
     * @param args
     */
    public static void main(String[] args) {
        DelayParser dp = new DelayParser("3=1000,2=10", 4, 4);
        Map m = dp.parse();
        System.out.println(m);
    }
}
