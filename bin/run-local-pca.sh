#!/bin/bash

# Figure out where we are.
FWDIR="$(cd `dirname $0`; pwd)"

## LOCAL TESTS

export OMP_NUM_THREADS=8 
export KEYSTONE_MEM=100g 

unset SPARK_HOME

# 1M by 128
# NUM_ROWS=1000000
# NUM_COLS=128
# LOCAL=true
# $FWDIR/run-pipeline.sh pipelines.general.PCAComparison --numRows $NUM_ROWS --numCols $NUM_COLS --local $LOCAL 2>&1 > pca-comparison-local-$LOCAL-rows-$NUM_ROWS-cols-$NUM_COLS.log
# 
# 10M by 128
# NUM_ROWS=10000000
# NUM_COLS=128
# LOCAL=true
# $FWDIR/run-pipeline.sh pipelines.general.PCAComparison --numRows $NUM_ROWS --numCols $NUM_COLS --local $LOCAL 2>&1 > pca-comparison-local-$LOCAL-rows-$NUM_ROWS-cols-$NUM_COLS.log
# 
# 2M by 1024
# NUM_ROWS=2000000
# NUM_COLS=1024
# LOCAL=true
# $FWDIR/run-pipeline.sh pipelines.general.PCAComparison --numRows $NUM_ROWS --numCols $NUM_COLS --local $LOCAL 2>&1 > pca-comparison-local-$LOCAL-rows-$NUM_ROWS-cols-$NUM_COLS.log
# 
# 10M by 1024
# NUM_ROWS=10000000
# NUM_COLS=1024
# LOCAL=true
# $FWDIR/run-pipeline.sh pipelines.general.PCAComparison --numRows $NUM_ROWS --numCols $NUM_COLS --local $LOCAL 2>&1 > pca-comparison-local-$LOCAL-rows-$NUM_ROWS-cols-$NUM_COLS.log

NUM_ROWS=100000
NUM_COLS=128
LOCAL=true
$FWDIR/run-pipeline.sh pipelines.general.PCAComparison --numRows $NUM_ROWS --numCols $NUM_COLS --local $LOCAL 2>&1 > pca-comparison-local-$LOCAL-rows-$NUM_ROWS-cols-$NUM_COLS.log
