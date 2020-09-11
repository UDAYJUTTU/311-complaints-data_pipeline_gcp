import airflow
from airflow.operators import bash_operator,PythonOperator
import os
from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.utils import trigger_rule
default_args = {
            'owner': 'Composer Example',
            'depends_on_past': False,
            'email': [''],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': datetime.timedelta(minutes=1),
            'start_date': datetime.datetime(2017, 1, 1),
}
def print_context(**kwargs):
    print(kwargs)
    file_name=kwargs['dag_run'].conf['name']
    bucket=kwargs['dag_run'].conf['bucket']
    bucket_path="gs://{}/{}".format(bucket,file_name)
    kwargs['ti'].xcom_push(key="bucket_path",value=bucket_path)

with airflow.DAG('gcs_composer_trigger_dag',default_args=default_args, schedule_interval=None) as dag:

     create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
        task_id='create_dataproc_cluster',
        cluster_name='composer-311-complaints-{{ ds_nodash }}',
        num_workers=2,
        region=models.Variable.get('region'),
        zone=models.Variable.get('gce_zone'),
        project_id=models.Variable.get('project_id'),
        master_machine_type='n1-standard-1',
        worker_machine_type='n1-standard-1')

     run_dataproc_job = dataproc_operator.DataProcPySparkOperator(
        task_id="run_dataproc_job",
        main="gs://311-complaints-spark_jobs/spark_job.py",
        cluster_name='composer-311-complaints-{{ ds_nodash }}',
        region=models.Variable.get('region'),
        dataproc_pyspark_jars=['gs://spark-lib/bigquery/spark-bigquery-latest.jar'],
        arguments=['gs://{{ dag_run.conf.get("bucket") }}/{{ dag_run.conf.get("name") }}'])

     delete_dataproc_cluster = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster',
        cluster_name='composer-311-complaints-{{ ds_nodash }}',
        project_id=models.Variable.get('project_id'),
        region=models.Variable.get('region'),
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

     bigquery_transformations=BigQueryOperator(
        sql='/sql/job.sql',
        task_id='bigquery_transformations',
        use_legacy_sql=False,
     )

     create_dataproc_cluster >> run_dataproc_job >> delete_dataproc_cluster >> bigquery_transformations
