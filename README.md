# Basic protocal
- DOWN

    inform current process to end immediately.
- SEND <destination: int> <msg length: int> <msg: byte[]>

    inform current process to send msg (msg length) to destination
- RECV <source: int> <byte: int>
    
    inform current process to receive msg to destination
## Header format
    Verb(4bytes)+[size(4bytes)]