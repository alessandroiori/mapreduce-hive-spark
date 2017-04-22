#!/bin/bash

USER_NAME="ambpl"

printf "\n- Remove tmp files:\n"
sudo rm -R /tmp/*

# kill all java process
printf "\n- Kill all java process:\n"
jps
#jps | awk '{if($2=="NameNode" || $2=="NodeManager" || $2=="ResourceManager" || $2=="SecondaryNameNode") print $1;}' | xargs kill
jps | awk '{print $1;}' | xargs kill


printf "\n- Format namenode:\n"
$HADOOP_HOME/bin/hdfs namenode -format

printf "\n- Start dfs:\n"
$HADOOP_HOME/sbin/start-dfs.sh

printf "\n- Make /user/${USER_NAME}:\n"
$HADOOP_HOME/bin/hdfs dfs -mkdir -p /user/${USER_NAME}
$HADOOP_HOME/bin/hdfs dfs -ls /user/

printf "\n- Start yarn:\n"
$HADOOP_HOME/sbin/start-yarn.sh

# show java process
printf '\n- Show all java process:\n'
jps
