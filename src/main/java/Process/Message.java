package Process;

import java.io.Serializable;
import java.net.InetSocketAddress;

public class Message implements Serializable{
    int Sender_ID;
    InetSocketAddress Sender_addr;
    String msg;
    String Serial;

    public Message(int ID, InetSocketAddress addr, String msg){
        this.Sender_ID = ID;
        this.Sender_addr = addr;
        this.msg = msg;
        this.Serial = this.Sender_ID +";"+ msg;
    }
}
