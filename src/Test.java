//import Process.Process;

import java.nio.ByteBuffer;

public class Test {
    public static void main(String[] args) {
        ByteBuffer a = ByteBuffer.allocate(10);
        a.putInt(10);
        a.flip();
        System.out.println(a.getInt());
    }
}
