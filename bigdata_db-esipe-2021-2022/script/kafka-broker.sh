#!/bin/sh

lib=~/lib
kafka_dir=$lib/kafka

broker_id=$1
port=$2

$kafka_dir/bin/kafka-server-start.sh \
  $kafka_dir/config/server.properties \
  --override broker.id=$broker_id \
  --override log.dirs=/tmp/kafka-logs-$broker_id \
  --override port=$port
