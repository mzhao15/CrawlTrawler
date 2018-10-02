from airflow import DAG
from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'meng',
    'depends_on_past': False,
    'start_date': datetime.now(),
    'email': ['cliffish0408@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2018, 10, 1),
}

# schedule_interval seems to be the interval between DAG runs
# schedule_interval=None
dag = DAG('mybatch_scheduler', default_args=default_args, schedule_interval=timedelta(days=1))


parent = None
taskdate = datetime(2016, 1, 1).date()
while taskdate < datetime(2016, 2, 1).date():
    '''
    run the batch processes on a daily base
    '''
    task1 = BashOperator(
        task_id='finding_{}'.format(taskdate),
        bash_command='cd /home/ubuntu/Insight-Meng/src/Spark/; ./CrawlerFinder.sh {{params.taskdate}}',
        params={'taskdate': str(taskdate)},
        dag=dag)

    if parent:
        task1.set_upstream(parent)

    task2 = BashOperator(
        task_id='counting_{}'.format(taskdate),
        bash_command='cd /home/ubuntu/Insight-Meng/src/Spark/; ./Total.sh {{params.taskdate}}',
        params={'taskdate': str(taskdate)},
        dag=dag)

    # task2 executes only after task1 is completed
    task2.set_upstream(task1)
    # next day relay on the previous day
    parent = task1
    # add one day increment
    taskdate = taskdate + timedelta(days=1)
