#!/usr/bin/env bash
gradle jar
if [ $(uname) = "Darwin" ]; then
    osascript -e "tell application \"Terminal\" to do script \"cd $PWD && java -cp build/libs/CS425MP1.jar Process.MasterUp 0 TotalConfiguration\""
else
    gnome-terminal --tab -x zsh -c "java -cp build/libs/CS425MP1.jar Process.MasterUp 0 TotalConfiguration"
fi
sleep 3
for id in $(seq 1 $1)
do
echo "$id"
if [ $(uname) = "Darwin" ]; then
    if [ -n "$2" ]; then
        osascript -e "tell application \"Terminal\" to do script \"cd $PWD && java -cp build/libs/CS425MP1.jar Process.TotalOrderDemo $id TotalConfiguration $2/$id\""
    else
        osascript -e "tell application \"Terminal\" to do script \"cd $PWD && java -cp build/libs/CS425MP1.jar Process.TotalOrderDemo $id TotalConfiguration\""
    fi
else
    if [ -n "$2" ]; then
        gnome-terminal --tab -x zsh -c "java -cp build/libs/CS425MP1.jar Process.TotalOrderDemo $id TotalConfiguration $2/$id"
    else
        gnome-terminal --tab -x zsh -c "java -cp build/libs/CS425MP1.jar Process.TotalOrderDemo $id TotalConfiguration"
    fi
fi
done