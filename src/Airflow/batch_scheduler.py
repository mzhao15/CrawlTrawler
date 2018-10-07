from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


'''
DAG used to run spark jobs in a year of 2016
'''

# DAG settings
default_args = {
    'owner': 'meng',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['cliffish0408@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

# schedule_interval: @daily
dag = DAG('mybatch_scheduler', default_args=default_args, schedule_interval=timedelta(days=1))


parent = None
taskdate = datetime(2016, 1, 1).date()
while taskdate < datetime(2017, 1, 1).date():
    # first task: sparkbatch_CrawlerFinder.py
    task1 = BashOperator(
        task_id='finding_{}'.format(taskdate),
        bash_command='cd /home/ubuntu/Insight-Meng/src/Spark/; ./CrawlerFinder.sh {{params.taskdate}}',
        params={'taskdate': str(taskdate)},
        dag=dag)

    # next day starts when current day finishes
    if parent:
        task1.set_upstream(parent)

    # second task: sparkbatch_Total.py
    task2 = BashOperator(
        task_id='counting_{}'.format(taskdate),
        bash_command='cd /home/ubuntu/Insight-Meng/src/Spark/; ./Total.sh {{params.taskdate}}',
        params={'taskdate': str(taskdate)},
        dag=dag)

    # task2 executes only when task1 finishes
    task2.set_upstream(task1)

    parent = task1
    # add one day increment
    taskdate = taskdate + timedelta(days=1)
