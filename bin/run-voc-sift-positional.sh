#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"
pushd $FWDIR

VOC_TRAIN_DIR="/VOCtrainval_06-Nov-2007.tar"
VOC_VAL_DIR="/VOCtest_06-Nov-2007.tar"
VOC_LABELS="src/test/resources/images/voclabels.csv"


DATE=`date +"%Y_%m_%d_%H_%M_%S"`
mkdir -p /mnt/logs

export SPARK_HOME=/root/spark

KEYSTONE_MEM=50g ./bin/run-pipeline.sh \
  pipelines.images.voc.VOCSIFTPositionalFisher \
  --trainLocation $VOC_TRAIN_DIR \
  --testLocation $VOC_VAL_DIR \
  --labelPath $VOC_LABELS \
  --numParts 200 2>&1 |  python ./bin/notify.py VOCSIFTPositionalFisher
popd
