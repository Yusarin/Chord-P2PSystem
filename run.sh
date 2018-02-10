#!/usr/bin/env bash
for id in $(seq 1 $1)
do
echo "$id"
gnome-terminal --tab -x zsh -c "java -cp build/libs/CS425MP1.jar Process.Process $id configuration"
done