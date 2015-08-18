HADOOP=/root/mapreduce/bin/hadoop

# Start mapreduce if necessary
/root/mapreduce/bin/start-mapred.sh
/root/ephemeral-hdfs/sbin/start-all.sh

#Copy to HDFS
$HADOOP distcp s3n://voc-data/VOCtrainval_06-Nov-2007.tar /voc-train
$HADOOP distcp s3n://voc-data/VOCtest_06-Nov-2007.tar /voc-test

