#!/bin/bash

#for k in 3 7 13 31 53 79 97
for n in 1000000 100000 10000
do
  for k in 2 4 8 16 32 64 128 256
  do
    OMP_NUM_THREADS=1 KEYSTONE_MEM=20g ./bin/run-pipeline.sh pipelines.general.GMMComparison --inputPath /mnt/gmmfeats.csv --numExamples $n --numCenters $k 2>&1 > ./gmm-comparison-$n-$k-centers-with-init-profile-omp-1.txt
  done
done
