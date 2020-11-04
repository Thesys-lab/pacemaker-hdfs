#!/usr/bin/env bash

sudo apt-get update

# hadoop runtime prerequisite
sudo apt-get -y install openjdk-8-jdk
sudo apt-get -y install zlib1g-dev pkg-config libssl-dev libsasl2-dev
sudo apt-get -y install snappy libsnappy-dev
sudo apt-get -y install bzip2 libbz2-dev
sudo apt-get -y install libjansson-dev
sudo apt-get -y install fuse libfuse-dev
sudo apt-get -y install zstd

sudo apt-get install pdsh

sudo echo "ssh" > /etc/pdsh/rcmd_default

sudo apt-get -y install libsnappy-dev

# DevOps utils
sudo apt-get -y install tree

# format and mount the missing disk space
sudo mkfs.ext4 /dev/sda4
sudo mount /dev/sda4 /tmp
sudo chmod 777 /tmp
