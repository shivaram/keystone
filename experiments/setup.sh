#!/bin/bash

git clone --single-branch --branch dag-estimation https://github.com/etrain/keystone.git /mnt/keystone

pushd /mnt/imagenet-keystone > /dev/null
cp ~/keystone/sbt/sbt sbt/.
make
sbt/sbt assembly
~/spark-ec2/copy-dir /mnt/keystone

mkdir -p /mnt2/log
mkdir -p /mnt2/pdfs
mkdir -p /mnt2/profiles

cd experiments
bash amazon/setup.sh
bash imagenet/setup.sh
bash timit/setup.sh
bash voc/setup.sh
cd -