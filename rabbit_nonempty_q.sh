#!/bin/bash

echo "QueueName QueueDepth Unacked ConsumerUtil ConsumerCount"
sudo rabbitmqctl list_queues name messages messages_unacknowledged consumer_utilisation consumers | awk '( $2 > 0 )'
