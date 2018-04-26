#!/usr/bin/env bash
gradle jar
for id in $(seq 1 $1)
do
echo "$id"
if [ $(uname) = "Darwin" ]; then
    osascript -e "tell application \"Terminal\" to do script \"cd $PWD && java -cp build/libs/Chord-P2PSystem.jar Process.RunClient Configuration\""
else
    gnome-terminal --tab -x zsh -c "java -cp build/libs/Chord-P2PSystem.jar Process.RunClient Configuration"
fi
done