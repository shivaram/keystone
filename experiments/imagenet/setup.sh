HADOOP=/root/mapreduce/bin/hadoop

# Start mapreduce if necessary
/root/mapreduce/bin/start-mapred.sh
/root/ephemeral-hdfs/sbin/start-all.sh

# Get the data from S3
$HADOOP distcp s3n://imagenet-train-all-scaled-tar/imagenet-train-all-scaled-tar /
$HADOOP distcp s3n://imagenet-validation-all-scaled-tar/imagenet-validation-all-scaled-tar /
