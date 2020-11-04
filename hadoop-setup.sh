#!/bin/bash
USER=$1
EXP=$2
PROJ=$3
CURRENT_DIR=$(pwd)
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HOME="/tmp/hadoop-3.2.0"
export PDSH_RCMD_TYPE=ssh
python3 config-setup.py disk_groups_config.json $USER-$EXP.$PROJ
export HOSTS="node[1-21].$USER-$EXP.$PROJ.apt.emulab.net"
/tmp/hadoop-3.2.0/bin/hdfs namenode -format
PDSH_RCMD_TYPE=ssh
/tmp/hadoop-3.2.0/sbin/start-dfs.sh
cp new_policy.xml /tmp/hadoop-3.2.0
/tmp/hadoop-3.2.0/bin/hdfs ec -addPolicies -policyFile $HADOOP_HOME/new_policy.xml
/tmp/hadoop-3.2.0/bin/hdfs ec -enablePolicy -policy RS-LEGACY-6-3-1024k
/tmp/hadoop-3.2.0/bin/hdfs ec -enablePolicy -policy RS-LEGACY-7-3-1024k
