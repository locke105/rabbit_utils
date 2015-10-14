#!/bin/bash

PIDS=$(sudo netstat -natp | grep 3306 | awk '$7 ~ /[^-]/ { print $7 }' | sort -u | awk -F '/' '{ print $1 }')

echo $PIDS

for pid in $PIDS; do
    ps aux | grep $pid
done
