#!/bin/bash

HOSTNAME=$(hostname)
sed -i "s/master.localdomain/$HOSTNAME/g" $HADOOP_CONF_DIR/core-site.xml
sed -i "s/master.localdomain/$HOSTNAME/g" $HADOOP_CONF_DIR/yarn-site.xml

mkdir -p /data/hadoop/hdfs/nn
mkdir -p /data/hadoop/hdfs/snn
mkdir -p /data/hadoop/hdfs/dnn

hdfs namenode -format

start-dfs.sh
start-yarn.sh

hdfs dfs -mkdir /user
hdfs dfs -mkdir /user/root

/usr/local/spark/sbin/start-all.sh
