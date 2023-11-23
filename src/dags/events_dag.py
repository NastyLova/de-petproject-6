import airflow
import os
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import date, datetime

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME'] = '/usr'
os.environ['SPARK_HOME'] = '/usr/lib/spark'
os.environ['PYTHONPATH'] = '/usr/local/lib/python3.8'

default_args = {
    'start_date': datetime(2020, 1, 1),
    'owner': 'airflow'
}

dag = DAG(
    dag_id='dag_events',
    schedule_interval='0 3 * * *',
    default_args=default_args
)

reload_events = SparkSubmitOperator(
    task_id='reload_events',
    dag=dag,
    application='/lessons/odd_events.py',
    conn_id='yarn_spark',
    application_args=[
        '/user/master/data/geo/events',
        '/user/lovalovan/data/geo/events'
    ],
    conf={
        "spark.driver.maxResultSize": "20g",
        "spark.driver.memory": "4g",
        "spark.driver.cores": "2",
        "spark.executor.cores": "4",
        "spark.executor.memory": "15g",
        "spark.executor.memoryOverhead": "2g"
    }
)

update_dm_events = SparkSubmitOperator(
    task_id='update_marts',
    dag=dag,
    application='/lessons/dm_events.py',
    conn_id='yarn_spark',
    application_args=[
        '/user/lovalovan/data/geo/events',
        '/user/lovalovan/analytics'
    ],
    conf={
        "spark.driver.maxResultSize": "20g",
        "spark.driver.memory": "4g",
        "spark.driver.cores": "2",
        "spark.executor.cores": "4",
        "spark.executor.memory": "15g",
        "spark.executor.memoryOverhead": "2g"
    },
    executor_cores=2,
    executor_memory='2g'
)


reload_events >> update_dm_events
