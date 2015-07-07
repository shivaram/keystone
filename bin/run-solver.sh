#!/bin/bash

FWDIR="$(cd `dirname $0`/..; pwd)"
pushd $FWDIR

#export SPARK_HOME="/root/spark"
export KEYSTONE_MEM=200g
export MASTER="local[16]"

FEATURES_TRAIN_DIR="/mnt/trainLabelsFeatures-all.csv"
FEATURES_VAL_DIR="/mnt/testLabelsFeatures-all.csv"
LABELS_TRAIN_DIR="/mnt/trainLabels-all.csv"
LABELS_ACTUAL_DIR="/mnt/testActual-all.csv"

export SPARK_LOCAL_DIRS="/mnt/spark,/mnt2/spark"
export OMP_NUM_THREADS=1
export SPARK_JAVA_OPTS="-XX:+PrintGCDetails -Dspark.mlmatrix.treeBranchingFactor=16 -Dspark.master=local[16] -Dspark.driver.maxResultSize=0 -Dspark.akka.frameSize=500 -Dspark.storage.memoryFraction=0.7"

LOG_SUFFIX=`date +"%Y_%m_%d_%H_%M_%S"`

./bin/run-pipeline.sh \
  pipelines.solvers.BlockWeightedSolver \
  --trainFeaturesDir $FEATURES_TRAIN_DIR \
  --trainLabelsDir $LABELS_TRAIN_DIR \
  --testFeaturesDir $FEATURES_VAL_DIR \
  --testActualDir $LABELS_ACTUAL_DIR \
  --lambda 6e-5 \
  --mixtureWeight 0.25 >& /mnt/solver-logs-"$LOG_SUFFIX".log
