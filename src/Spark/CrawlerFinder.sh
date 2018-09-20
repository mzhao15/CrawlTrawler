#!/bin/bash

cd /usr/local/spark/bin/

spark-submit --master spark://ip-10-0-0-6:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.2.0 --executor-memory 4G sparkbatch_CrawlerFinder.py
