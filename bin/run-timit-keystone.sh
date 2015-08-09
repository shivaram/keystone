#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"
pushd $FWDIR

export TRAIN_DIR=/timit-train-features.csv
export TRAIN_LABELS=/mnt/timit-train-labels.sparse
export TEST_DIR=/timit-test-features.csv
export TEST_LABELS=/mnt/timit-test-labels.sparse
export ITERS=1
if [ $# -eq 1 ];
then
  echo "Running weight $1"
  export WEIGHT=$1
else
  export WEIGHT=0.25
fi

LOG_SUFFIX=`date +"%Y_%m_%d_%H_%M_%S"`

./bin/run-pipeline.sh \
  pipelines.speech.TimitPipeline \
  --trainDataLocation $TRAIN_DIR \
  --trainLabelsLocation $TRAIN_LABELS \
  --testDataLocation $TEST_DIR \
  --testLabelsLocation $TEST_LABELS \
  --mixtureWeight $WEIGHT \
  --numCosines 1 \
  --numParts 256 \
  --numEpochs $ITERS 2>&1 | tee /mnt/timit-weighted-logs-"$LOG_SUFFIX"-iters-$ITERS-weight-$WEIGHT.log

popd
