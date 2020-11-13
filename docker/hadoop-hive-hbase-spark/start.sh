#!/bin/bash

docker-compose -f bigdata-docker-compose.yml up -d namenode datanode1 datanode2 datanode3 resourcemanager nodemanager1 historyserver

sleep 5

docker-compose -f bigdata-docker-compose.yml up -d hive-metastore-postgresql hive-server hive-metastore hive-metastore-postgresql mysql-server

sleep 5

docker-compose -f bigdata-docker-compose.yml up -d spark-master spark-worker

sleep 5

docker-compose -f bigdata-docker-compose.yml up -d spark-master spark-worker

sleep 5

docker-compose -f bigdata-docker-compose.yml up -d elasticsearch kibana

sleep 5

docker-compose -f docker-compose.yml up -d zookeeper kafka kafka-manager

sleep 5

docker-compose -f docker-compose.yml up -d zookeeper kafka kafka-manager

sleep 5

docker-compose -f docker-compose.yml up -d jobmanager taskmanager