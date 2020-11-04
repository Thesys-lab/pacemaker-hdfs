#!/bin/bash
USER=$1
EXP=$2
PROJ=$3
for number in {1..20}
do
scp node-setup.sh node$number.$USER-$EXP.$PROJ.apt.emulab.net:~/
done
exit 0
