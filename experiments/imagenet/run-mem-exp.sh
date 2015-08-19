#!/bin/bash

LOGDIR=/mnt2/log
PROF_DIR=/mnt2/profiles
PDF_DIR=/mnt2/pdfs
DATE=`date "+%Y%m%d.%H%M%S"`
NUM_WORKERS=`cat /root/spark/conf/slaves | wc -l`
NUM_CORES=$(( `cat /proc/cpuinfo | grep processor | wc -l` / 2 ))
NUM_PARTITIONS=$(( $NUM_WORKERS * $NUM_CORES ))

TRAIN_DIR=/imagenet-train-all-scaled-tar
TEST_DIR=/imagenet-validation-all-scaled-tar
TRAIN_LABEL_PATH=/timit-train-labels.sparse
TEST_LABEL_PATH=/timit-test-labels.sparse

EXP=ImageNet

#Do TIMIT
for MEM in 80g 40g 20g 10g 5g 2.5g 1g 500m
do
  for STRAT in Greedy EstOnly All
  do
    LOGFILE=$LOGDIR/$EXP.$STRAT.$MEM.$DATE.log
  
    KEYSTONE_MEM=$MEM bash bin/run-pipeline.sh workflow.OptimizerEvaluator \
      --trainLocation $TRAIN_DIR \
      --trainLabels $TRAIN_LABEL_PATH \
      --testLocation $TEST_DIR \
      --testLabels $TEST_LABEL_PATH \
      --memSize $MEM \
      --testPipeline $EXP \
      --numPartitions $NUM_PARTITIONS \
      --numWorkers $NUM_WORKERS \
      --cachingStrategy $STRAT \
      --profilesDir $PROF_DIR \
      --pdfDir $PDF_DIR \
      --sampleSizes 2,4 >$LOGFILE 2>&1
  done
done