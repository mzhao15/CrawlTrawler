#!/bin/bash

cd src/Spark/

case "$1" in

    finding)

    ./CrawlerFinder.sh $2
    ;;

    counting)

    ./Total.sh $2
    ;;

    *)

    echo "Usage finding|counting 2016-01-10"
    ;;

esac
