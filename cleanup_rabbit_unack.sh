#!/bin/bash

set -e

conn_names=$(sudo rabbitmqctl list_channels pid connection consumer_count messages_unacknowledged | awk '($4 > 0) {print $2}')

for conn in $conn_names; do
    echo "Cleaning up $conn"
    sudo rabbitmqctl close_connection "$conn" "bad ack"
done
