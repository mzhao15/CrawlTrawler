from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


'''
DAG used to run spark jobs on a daily basis
'''

# DAG settings
default_args = {
    'owner': 'meng',
    'depends_on_past': False,
    'start_date': datetime(2016, 1, 1),
    'end_date': datetime(2016, 12, 31),
    'email': ['cliffish0408@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

# schedule_interval: @daily
dag = DAG('daily_scheduler', default_args=default_args, schedule_interval=timedelta(days=1))

task1 = BashOperator(
    task_id='finding',
    bash_command='cd /home/ubuntu/Insight-Meng/src/Spark/; ./CrawlerFinder.sh {{ ds }} ',
    dag=dag)

# second task: sparkbatch_Total.py
task2 = BashOperator(
    task_id='counting',
    bash_command='cd /home/ubuntu/Insight-Meng/src/Spark/; ./Total.sh {{ ds }} ',
    dag=dag)

# task2 executes only when task1 finishes
task2.set_upstream(task1)
