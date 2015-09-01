#!/bin/bash

# Script to copy data from one cluster to another in batches via Blueshift Incrementally

ts=`date +%s`

src_path=/projects/bigfoot/ingestion/data/core-batch/*/hourly

date_path=`date -d '1 hour ago' "+%Y/%m/%d/%H"`

src_path=hdfs://zsnn1.nm.flipkart.com${src_path}/${date_path}

hadoop fs -ls ${src_path} | awk '{print $8}' > /tmp/blueshift-input/include.list

config_file=dcmconfig.json
queue=critical
user=fk-bigfoot-azkaban
log_file=/var/log/blueshift/bs_mirror_cb_${ts}

echo "Starting Core Batch Mirror - "${date_path}

export HADOOP_USER_NAME=${user}

echo "Processing Path: ${src_path}"

hadoop jar blueshift-0.0.1-SNAPSHOT.jar -Dmapreduce.job.queuename=${queue} -libjars /usr/share/fk-bigfoot-4mc/lib/hadoop-4mc-1.1.0.jar -P${config_file} &>${log_file}

echo "Done - Core Batch Mirror - "${date_path}

