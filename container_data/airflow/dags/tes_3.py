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
VENV_PATH = "/opt/airflow/venv_dir/venv_1/bin/python"
JOB_DIR = "/opt/airflow/job_dir/tes_3"

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
    dag_id="tes_3",
    description='latihan 3',
    default_args={
        'depends_on_past': False,
        'retries': 1,
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
        t_rfam__motif_old = ExternalPythonOperator(task_id="rfam__motif_old",python_callable=run_job,op_kwargs={"module_name": "rfam__motif_old","job_dir": JOB_DIR},python=VENV_PATH)
        t_rfam__wikitext = ExternalPythonOperator(task_id="rfam__wikitext",python_callable=run_job,op_kwargs={"module_name": "rfam__wikitext","job_dir": JOB_DIR},python=VENV_PATH)
        t_rfam__taxonomy = ExternalPythonOperator(task_id="rfam__taxonomy",python_callable=run_job,op_kwargs={"module_name": "rfam__taxonomy","job_dir": JOB_DIR},python=VENV_PATH)
        t_rfam__literature_reference = ExternalPythonOperator(task_id="rfam__literature_reference",python_callable=run_job,op_kwargs={"module_name": "rfam__literature_reference","job_dir": JOB_DIR},python=VENV_PATH)

    with TaskGroup("tier_2", tooltip="tier 2 dependency") as tier_2:
        t_rfam__family = ExternalPythonOperator(task_id="rfam__family",python_callable=run_job,op_kwargs={"module_name": "rfam__family","job_dir": JOB_DIR},python=VENV_PATH)
        t_rfam__motif_literature = ExternalPythonOperator(task_id="rfam__motif_literature",python_callable=run_job,op_kwargs={"module_name": "rfam__motif_literature","job_dir": JOB_DIR},python=VENV_PATH)
        t_rfam__rfamseq = ExternalPythonOperator(task_id="rfam__rfamseq",python_callable=run_job,op_kwargs={"module_name": "rfam__rfamseq","job_dir": JOB_DIR},python=VENV_PATH)

    with TaskGroup("tier_3", tooltip="tier 3 dependency") as tier_3:
        t_rfam__motif_matches = ExternalPythonOperator(task_id="rfam__motif_matches",python_callable=run_job,op_kwargs={"module_name": "rfam__motif_matches","job_dir": JOB_DIR},python=VENV_PATH)

    end = EmptyOperator(task_id="end")
    
    
    start >> tier_1 
    
    # tier 1 downstream configuration
    t_rfam__wikitext >> t_rfam__family
    t_rfam__motif_old >> [t_rfam__motif_matches,t_rfam__motif_literature]
    t_rfam__taxonomy >> t_rfam__rfamseq
    t_rfam__literature_reference >> t_rfam__motif_literature
    
    # tier 2 downstream configuration
    t_rfam__family >> t_rfam__motif_matches
    t_rfam__rfamseq >> t_rfam__motif_matches
    t_rfam__motif_literature >> end
    
    # tier 3 downstream configuration
    t_rfam__motif_matches >> end
