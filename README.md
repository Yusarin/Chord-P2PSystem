# Basic schema
- Each process is a java process

- Each process has one thread to handle user Input, one thread to send message,
and one thread for receiving message from its peers.

- There is a master node to handle total order multicast.

- Each process maintain an internal vector clock

##Requirement
- MacOs or Linux

- Java 9 installed

- Gradle installed

## Project structure


## Build
> gradle jar (at root dir)

## Run
Run CausalMulticastDemo by .jar after build
> java -cp build/libs/CS425MP1.jar Process.CausalMulticastDemo \<id\> configuration [script]

Run UnicastDemo by .jar after build
> java -cp build/libs/CS425MP1.jar Process.UnicastDemo \<id\> configuration

> ./UnicastRun.sh 4(the number of process, has to be consistent to configuration file)

> ./CausalMulticastRun.sh 4(the number of process, has to be consistent to configuration 
file) [script (read startup command from ./script directory)]

## Available command

### Unicast
> send \<id\> \<message\>

### Causal multicast

> sleep (sleep for 1000ms)

> clock (check current vector clock)

> msend \<message\> [delay id=delay,id=delay...(the last delay will be used to other unspecified
processes, current thread is always 0 (I think that make sense))]

### Exit
Just press Ctrl+C

## Script format

Same as available command (script is only available for causal multicast)
