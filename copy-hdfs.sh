#!/bin/bash
USER=$1
EXP=$2
PROJ=$3
for number in {1..20}
do
scp -r /tmp/hadoop-3.2.0 node$number.$USER-$EXP.$PROJ.apt.emulab.net:/tmp/
done
exit 0
