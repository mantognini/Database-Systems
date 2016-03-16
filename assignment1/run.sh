#!/bin/bash

if [ -z $1 ]
then
        echo "Expected input argument for dataset size" 1>&2
        exit 1
fi

hadoop fs -rm -r -f '/user/mantogni/output/out'

sbt package

spark-submit --class "main.scala.Project1" \
        --master yarn-cluster --num-executors 25 \
        Project1.jar \
        hdfs:///datasets/tpch/${1}gb \
        hdfs:///user/mantogni/output

hadoop fs -cat '/user/mantogni/output/out/*' | sort -n

hadoop fs -get '/spark-history/application_*' /home/mantogni/logs/ 2>/dev/null

