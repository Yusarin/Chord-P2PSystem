package Process;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class Message implements Serializable{
    int Sender_ID;
    int header;
    InetSocketAddress Sender_addr;
    long customDelay;
    String msg;
    String Serial;

    public Message(int ID, InetSocketAddress addr, String msg, int header, long customDelay){
        this.Sender_ID = ID;
        this.Sender_addr = addr;
        this.header = header;
        this.msg = msg;
        this.Serial = this.Sender_ID + "," + this.header +","+ msg;
        this.customDelay = customDelay;
    }
}
