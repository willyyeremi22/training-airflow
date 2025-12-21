##################################################
# import airflow library
##################################################
from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import ExternalPythonOperator
from airflow.utils.task_group import TaskGroup
from pendulum import datetime, timezone

##################################################
# import default library
##################################################
from datetime import timedelta

##################################################

##################################################
# global variable
##################################################
VENV_PATH = "/home/airflow/venv_dir/venv_1/bin/python"

##################################################
# task definition
##################################################
def dbo__current_cc_scmaccp():
    import sys
    sys.path.insert(0, "/home/airflow/job_dir/tes_2")
    from dbo__current_cc_scmaccp import main
    main()

##################################################
# DAG configuration
##################################################
with DAG(
    dag_id="tes_2",
    description='latihan 2',
    default_args={
        'depends_on_past': False,
        'retries': 3,
        'retry_delay': timedelta(seconds=5),
    },
    start_date = datetime(2025, 12, 17, tz=timezone("Asia/Jakarta")),
    schedule = "30 7 * * *",
    catchup=False,
    max_active_runs=2,
    max_active_tasks=2,
    orientation="TB",
    tags=["latihan"]
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("tier_1", tooltip="tier 1 dependency") as tier_1:
        t_dbo__current_cc_scmaccp = ExternalPythonOperator(task_id="dbo__current_cc_scmaccp",python_callable=dbo__current_cc_scmaccp,python=VENV_PATH)

    end = EmptyOperator(task_id="end")
    
    start >> tier_1 >> end
