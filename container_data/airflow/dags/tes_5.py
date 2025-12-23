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
VENV_PATH = "/opt/airflow/venv_dir/venv_2/bin/python"
JOB_DIR = "/opt/airflow/job_dir/tes_5"

##################################################
# task definition
##################################################
def run_job(module_name: str, job_dir: str):
    from sys import path
    from importlib import import_module
    if job_dir not in path:
        path.insert(0, job_dir)
    module = import_module(module_name)
    if not hasattr(module, "main"):
        raise AttributeError(f"Module '{module_name}' tidak punya fungsi main()")
    module.main()

##################################################
# DAG configuration
##################################################
with DAG(
    dag_id="tes_5",
    description='latihan 5',
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
        t_rnacen__ontology_terms = ExternalPythonOperator(task_id="rnacen__ontology_terms",python_callable=run_job,op_kwargs={"module_name": "rnacen__ontology_terms","job_dir": JOB_DIR},python=VENV_PATH)
        t_rnacen__rfam_clans = ExternalPythonOperator(task_id="rnacen__rfam_clans",python_callable=run_job,op_kwargs={"module_name": "rnacen__rfam_clans","job_dir": JOB_DIR},python=VENV_PATH)
        t_rnacen__rfam_models = ExternalPythonOperator(task_id="rnacen__rfam_models",python_callable=run_job,op_kwargs={"module_name": "rnacen__rfam_models","job_dir": JOB_DIR},python=VENV_PATH)
        t_rnacen__rnc_release = ExternalPythonOperator(task_id="rnacen__rnc_release",python_callable=run_job,op_kwargs={"module_name": "rnacen__rnc_release","job_dir": JOB_DIR},python=VENV_PATH)
        
    end = EmptyOperator(task_id="end")
    
    start >> tier_1 >> end
