#!/bin/env bash

set -e
# keep track of the last executed command
trap 'last_command=$current_command; current_command=$BASH_COMMAND' DEBUG
# echo an error message before exiting
trap 'echo "\"${last_command}\" command filed with exit code $?."' EXIT


source params.sh
echo user ${user} experiemnt ${exp} ${proj}

echo '############ setup config #############' 
cp ssh_config ~/.ssh/config
echo "ssh" | sudo tee /etc/pdsh/rcmd_default >/dev/null

echo '############ updating core-site.xml #############' 
sed -i "s/saukad-qv79471.redundancy-pg0/${user}-${exp}.${proj}/g" hadoop-common-project/hadoop-common/src/main/conf/core-site.xml 

echo '############ updating hdfs-site.xml #############' 
sed -i "s/saukad-qv79471.redundancy-pg0/${user}-${exp}.${proj}/g" hadoop-hdfs-project/hadoop-hdfs/src/main/conf/hdfs-site.xml 

echo '############ build hadoop #############' 
bash build-hadoop.sh 

echo '############ copy script #############' 
bash copy-script.sh ${user} ${exp} ${proj}

echo '############ setup other nodes #############' 
bash setup-other-nodes.sh ${user} ${exp} ${proj}

echo '############ copy hdfs #############' 
bash copy-hdfs.sh ${user} ${exp} ${proj}

echo '############ hadoop setup #############' 
bash hadoop-setup.sh ${user} ${exp} ${proj}

echo '############ checking hadoop #############' 
res=`/tmp/hadoop-3.2.0/bin/hdfs dfsadmin -report | grep "Live datanodes" | awk '{print $3}'`
if [[ "$res" != "(20):" ]]; then 
    echo ${res}, hadoop setup error 
else
    echo "hadoop setup is done"
fi
