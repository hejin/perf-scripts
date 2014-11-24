#!/bin/bash
me=`whoami`
if [ $me != "root" ]; then
   echo "Warning: Please run it with root account."
   echo "Quit running."
   exit 1
fi

python ./io_stat.py $1 $2 $3 $4
