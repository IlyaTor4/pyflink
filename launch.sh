#!/bin/sh

./bin/start-cluster.sh
flink run -py /opt/flink/main.py
