#!/bin/bash
pid=`cat /var/run/iosampler.pid`
echo $pid
if [ $pid"x" == "x" ]; then
    echo "sample process doesn't start, abort ..."
    exit 1
fi

me=`whoami`
if [ $me != "root" ]; then
   echo "Warning: Please run it with root account."
   echo "Quit running."
   exit 1
fi

kill -SIGTERM $pid
