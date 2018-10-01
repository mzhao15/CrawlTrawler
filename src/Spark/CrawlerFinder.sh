#!/bin/bash

FOLDER=$PWD
cd /usr/local/spark/bin/

spark-submit --master spark://ip-10-0-0-4:7077 \
 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 \
 --driver-memory 4G \
 --executor-memory 4G \
 $FOLDER/sparkbatch_CrawlerFinder.py $1
