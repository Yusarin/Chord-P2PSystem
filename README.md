# Basic protocal
- SHUTDOWN

    inform current process to end immediately.
- SEND <destination: int> <byte: int> <msg: byte[]>

    inform current process to send msg to destination
- RECV <source: int> <byte: int> <msg: byte[]>
    
    inform current process to receive msg to destination