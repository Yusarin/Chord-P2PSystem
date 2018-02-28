#!/usr/bin/env bash
for id in $(seq 1 $1)
do
echo "$id"
echo $(uname)
if [ $(uname) = "Darwin" ]; then
    if [ -n "$2" ]; then
        echo "$2/$id"
        osascript -e "tell application \"Terminal\" to do script \"cd $PWD && java -cp build/libs/CS425MP1.jar Process.CausalMulticastDemo $id configuration $2/$id\""
    else
        osascript -e "tell application \"Terminal\" to do script \"cd $PWD && java -cp build/libs/CS425MP1.jar Process.CausalMulticastDemo $id configuration\""
    fi
else
    if [ -n "$2" ]; then
        echo "$2/$id"
        gnome-terminal --tab -x zsh -c "java -cp build/libs/CS425MP1.jar Process.CausalMulticastDemo $id configuration $2/$id"
    else
        gnome-terminal --tab -x zsh -c "java -cp build/libs/CS425MP1.jar Process.CausalMulticastDemo $id configuration"
    fi
fi
#if (uname)
#if [ -n "$2" ]; then
#    echo "$2/$id"
#    gnome-terminal --tab -x zsh -c "java -cp build/libs/CS425MP1.jar Process.CausalMulticastDemo $id configuration $2/$id"
#else
#    gnome-terminal --tab -x zsh -c "java -cp build/libs/CS425MP1.jar Process.CausalMulticastDemo $id configuration"
#fi
done
