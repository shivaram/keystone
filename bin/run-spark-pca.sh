#!/bin/bash

# Figure out where we are.
FWDIR="$(cd `dirname $0`; pwd)"

## LOCAL TESTS

export OMP_NUM_THREADS=8 
export KEYSTONE_MEM=100g 

export SPARK_HOME=/root/spark
LOCAL=false
NUM_PARTS=256

DATE=`date +%Y_%m_%d_%H_%M_%S`

# 1M by 128
#NUM_ROWS=1000000
#NUM_COLS=128
#$FWDIR/run-pipeline.sh pipelines.general.PCAComparison --numParts $NUM_PARTS --numRows $NUM_ROWS --numCols $NUM_COLS --local $LOCAL 2>&1 > pca-comparison-local-$LOCAL-rows-$NUM_ROWS-cols-$NUM_COLS-$DATE.log
#
#10M by 128
#NUM_ROWS=10000000
#NUM_COLS=128
#$FWDIR/run-pipeline.sh pipelines.general.PCAComparison  --numParts $NUM_PARTS --numRows $NUM_ROWS --numCols $NUM_COLS --local $LOCAL 2>&1 > pca-comparison-local-$LOCAL-rows-$NUM_ROWS-cols-$NUM_COLS-$DATE.log
#
#2M by 1024
#NUM_ROWS=2000000
#NUM_COLS=1024
#$FWDIR/run-pipeline.sh pipelines.general.PCAComparison  --numParts $NUM_PARTS --numRows $NUM_ROWS --numCols $NUM_COLS --local $LOCAL 2>&1 > pca-comparison-local-$LOCAL-rows-$NUM_ROWS-cols-$NUM_COLS-$DATE.log
#
#10M by 1024
#NUM_ROWS=10000000
#NUM_COLS=1024
#$FWDIR/run-pipeline.sh pipelines.general.PCAComparison  --numParts $NUM_PARTS --numRows $NUM_ROWS --numCols $NUM_COLS --local $LOCAL 2>&1 > pca-comparison-local-$LOCAL-rows-$NUM_ROWS-cols-$NUM_COLS-$DATE.log

NUM_ROWS=100000
NUM_COLS=128
$FWDIR/run-pipeline.sh pipelines.general.PCAComparison --numParts $NUM_PARTS --numRows $NUM_ROWS --numCols $NUM_COLS --local $LOCAL 2>&1 > pca-comparison-local-$LOCAL-rows-$NUM_ROWS-cols-$NUM_COLS-$DATE.log
