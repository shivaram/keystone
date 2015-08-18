#!/bin/bash

LOGDIR=/mnt2/log
PROF_DIR=/mnt2/profiles
PDF_DIR=/mnt2/pdfs
DATE=`date "+%Y%m%d.%H%M%S"`
NUM_WORKERS=`cat /root/spark/conf/slaves | wc -l`
NUM_CORES=$(( `cat /proc/cpuinfo | grep processor | wc -l` / 2 ))
NUM_PARTITIONS=$(( $NUM_WORKERS * $NUM_CORES ))

TRAIN_DIR=/voc/VOCtrainval_06-Nov-2007.tar
TEST_DIR=/voc/VOCtest_06-Nov-2007.tar
LABEL_PATH=/root/keystone/src/test/resources/images/voclabels.csv

EXP=VOC

#Do VOC
for MEM in 80g 40g 20g 10g 5g 2.5g 1g 500m
do
  for STRAT in Greedy EstOnly All
  do
    LOGFILE=$LOGDIR/$EXP.$STRAT.$MEM.$DATE.log
    
    KEYSTONE_MEM=$MEM bash bin/run-pipeline.sh workflow.OptimizerEvaluator \
      --trainLocation $TRAIN_DIR \
      --trainLabels $LABEL_PATH \
      --testLocation $TEST_DIR \
      --testLabels $LABEL_PATH \
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
