#Archivo parent_dag

from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago

args = {
    'owner': 'airflow',
}

with DAG(
    dag_id='Car_rental_parent',  
    default_args=args,
    schedule_interval='0 0 * * *',
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=['ingest', 'trigger_target'],
    params={"example_key": "example_value"},
) as dag:

    ingest = BashOperator(
        task_id='ingesta',
        bash_command='/usr/bin/sh /home/hadoop/scripts/rev_ingest_car.sh ',
    )

    trigger_target = TriggerDagRunOperator(
        task_id = 'trigger_target',
        trigger_dag_id = 'Car_rental_child',
        execution_date = '{{ ds }}',
        reset_dag_run = True,
        wait_for_completion = True,
        poke_interval = 30
    )

    ingest >> trigger_target

if __name__ == "__main__":
    dag.cli()




