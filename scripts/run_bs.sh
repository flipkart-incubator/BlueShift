#!/bin/bash

# Script to copy data from one cluster to another in batches via Blueshift

# enable the below line if the job is running on the source cluster
# else manually create the topics.list file with line delimited locations to transfer
# hadoop fs -ls ${src_base_path} | awk '{print $8}' | awk -F"/" '{print $NF}' > topics.list

filename='topics.list'

filelines=`cat $filename`

ts=`date +%s`
export HADOOP_USER_NAME=admin
batch_name=FDP_GA1_TO_BHEEMA_01

echo "Starting batch ${batch_name}"

cp dcmconfig.json driver.json

for line in $filelines ; do

    ts=`date +%s`
    echo "Start copy - ${ts}"

    sed -i.bak "s/${batch_name}/${ts}/g" driver.json
    batch_name=${ts}

    echo "${line}" > include.list
    echo "Running : hadoop blueshift ${line} "
    hadoop jar blueshift.jar -Dmapreduce.job.queuename=admin -libjars /usr/share/fk-bigfoot-4mc/lib/hadoop-4mc-1.1.0.jar -Pdriver.json &

    sleep 10

    NPROC=$(($NPROC+1))
    if [ "$NPROC" -ge 20 ]; then
#        wait
        sleep 300
        NPROC=0
    fi

done

echo "Done Batch - ${ts}"
