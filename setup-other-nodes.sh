#!/bin/bash
USER=$1
EXP=$2
PROJ=$3
PDSH_RCMD_TYPE=ssh
HOSTS="node[1-20].$USER-$EXP.$PROJ.apt.emulab.net"
pdsh -w $HOSTS bash ~/node-setup.sh
