#!/bin/env bash

user=$(echo $(hostname)|cut -d '.' -f 2 | cut -d '-' -f 1)
exp=$(echo $(hostname)|cut -d '.' -f 2 | cut -d '-' -f 2)
proj=$(echo $(hostname)|cut -d '.' -f 3)

for i in `seq 0 20`; do 
  ping -c1 node${i}.${user}-${exp}.${proj}.apt.emulab.net >/dev/null
  status=$?
  if (( ${status} != 0 )); then 
      echo invalid params: status ${status}, FQDN node${i}.${user}-${exp}.${proj}.apt.emulab.net; 
  fi
done 

# echo parameters accepted
