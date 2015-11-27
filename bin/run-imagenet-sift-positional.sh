#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"
pushd $FWDIR

IMAGENET_TRAIN_DIR="/imagenet-train-all-scaled-tar"
IMAGENET_VAL_DIR="/imagenet-validation-all-scaled-tar"
IMAGENET_LABELS="/root/keystone/src/main/resources/imagenet-labels"

DATE=`date +"%Y_%m_%d_%H_%M_%S"`
mkdir -p /mnt/logs

export SPARK_HOME=/root/spark

KEYSTONE_MEM=150g ./bin/run-pipeline.sh \
  pipelines.images.imagenet.LazyImageNetSiftPositionalLcsFV \
  --trainLocation $IMAGENET_TRAIN_DIR \
  --testLocation $IMAGENET_VAL_DIR \
  --labelPath $IMAGENET_LABELS \
  --numPcaSamples 10000000 \
  --numGmmSamples 10000000 \
  --vocabSize 1024 \
  --centroidBatchSize 16 2>&1 | tee /mnt/logs/imagenet-sift-positional-$DATE-vocab-1024.log
popd
