#!/bin/bash

FOLDER=$AIRFLOW_HOME/dags
FILE=scheduler.py

if [ ! -d $FOLDER ]; then

    mkdir $FOLDER

fi

cp $FILE $FOLDER/
python $FOLDER/$FILE

airflow initdb
airflow webserver -p 8081
airflow scheduler
