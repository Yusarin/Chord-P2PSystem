package Process;

import java.io.*;
import java.util.*;
import java.net.*;
import java.nio.*;

public class Message implements Serializable{
    int Sender_ID;
    int header;
    InetSocketAddress Sender_addr;
    String msg;
    String Serial;

    public Message(int ID, InetSocketAddress addr, String msg, int header){
        this.Sender_ID = ID;
        this.Sender_addr = addr;
        this.header = header;
        this.msg = msg;
        this.Serial = this.Sender_ID + "," + this.header +","+ msg;
    }
}
