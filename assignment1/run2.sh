#!/bin/bash

set -e

if [ -z $1 ]
then
        echo "Expected input argument for task id" 1>&2
        exit 1
fi

if [ -z $2 ]
then
        echo "Expected input argument for dataset size" 1>&2
        exit 1
fi

hadoop fs -rm -r -f '/user/mantogni/output/out_'$1

sbt package

spark-submit --class "main.scala.Project2" \
        --master yarn-cluster --num-executors 10 \
        Project2.jar \
        hdfs:///datasets/tpch/${2}gb \
        hdfs:///user/mantogni/output \
        $1

hadoop fs -cat "/user/mantogni/output/out_${1}/"'*' | sort -n | tee result.tmp.txt

if [ -f result.previous.txt ]
then
        set +e
        cmp result.previous.txt result.tmp.txt
        if [ $? -eq 0 ]
        then
                echo "MATCH PREVIOUS"
        else
                echo "DIDN'T MATCH PREVIOUS"
        fi
        set -e

        rm result.previous.txt
else
        echo "WARNING: NO PREVIOUS"
fi

mv result.tmp.txt result.previous.txt

hadoop fs -get '/spark-history/application_*' /home/mantogni/logs/ 2>/dev/null || true

echo "SUCCESS"
