#!/usr/bin/env bash
gradle jar
for id in $(seq 1 $1)
do
echo "$id"
if [ $(uname) = "Darwin" ]; then
    osascript -e "tell application \"Terminal\" to do script \"cd $PWD && java -cp build/libs/CS425MP1.jar Process.UnicastDemo $id UnicastConfiguration\""
else
    gnome-terminal --tab -x zsh -c "java -cp build/libs/CS425MP1.jar Process.UnicastDemo $id UnicastConfiguration"
fi
done