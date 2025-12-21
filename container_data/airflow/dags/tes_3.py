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
def rnacen__ontology_terms():
    import sys
    sys.path.insert(0, "/home/airflow/job_dir/tes_3")
    from rnacen__ontology_terms import main
    main()

def rnacen__rfam_clans():
    import sys
    sys.path.insert(0, "/home/airflow/job_dir/tes_3")
    from rnacen__rfam_clans import main
    main()

def rnacen__rnc_release():
    import sys
    sys.path.insert(0, "/home/airflow/job_dir/tes_3")
    from rnacen__rnc_release import main
    main()

def rnacen__rfam_models():
    import sys
    sys.path.insert(0, "/home/airflow/job_dir/tes_3")
    from rnacen__rfam_models import main
    main()

def rnacen__rnc_taxonomy():
    import sys
    sys.path.insert(0, "/home/airflow/job_dir/tes_3")
    from rnacen__rnc_taxonomy import main
    main()
    
def rnacen__rnc_accessions():
    import sys
    sys.path.insert(0, "/home/airflow/job_dir/tes_3")
    from rnacen__rnc_accessions import main
    main()
    
def rnacen__rnc_rna_precomputed():
    import sys
    sys.path.insert(0, "/home/airflow/job_dir/tes_3")
    from rnacen__rnc_rna_precomputed import main
    main()

def rnacen__rnc_interactions():
    import sys
    sys.path.insert(0, "/home/airflow/job_dir/tes_3")
    from rnacen__rnc_interactions import main
    main()

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
        t_rnacen__ontology_terms = ExternalPythonOperator(task_id="rnacen__ontology_terms",python_callable=rnacen__ontology_terms,python=VENV_PATH)
        t_rnacen__rfam_clans = ExternalPythonOperator(task_id="rnacen__rfam_clans",python_callable=rnacen__rfam_clans,python=VENV_PATH)
        t_rnacen__rfam_models = ExternalPythonOperator(task_id="rnacen__rfam_models",python_callable=rnacen__rfam_models,python=VENV_PATH)
        t_rnacen__rnc_release = ExternalPythonOperator(task_id="rnacen__rnc_release",python_callable=rnacen__rnc_release,python=VENV_PATH)

    with TaskGroup("tier_2", tooltip="tier 2 dependency") as tier_2:
        t_rnacen__rnc_taxonomy = ExternalPythonOperator(task_id="rnacen__rnc_taxonomy",python_callable=rnacen__rnc_taxonomy,python=VENV_PATH)
        t_rnacen__rnc_accessions = ExternalPythonOperator(task_id="rnacen__rnc_accessions",python_callable=rnacen__rnc_accessions,python=VENV_PATH)
        t_rnacen__rnc_rna_precomputed = ExternalPythonOperator(task_id="rnacen__rnc_rna_precomputed",python_callable=rnacen__rnc_rna_precomputed,python=VENV_PATH)

    with TaskGroup("tier_3", tooltip="tier 3 dependency") as tier_3:
        t_rnacen__rnc_interactions = ExternalPythonOperator(task_id="rnacen__rnc_interactions",python_callable=rnacen__rnc_interactions,python=VENV_PATH)

    end = EmptyOperator(task_id="end")
    
    
    start >> tier_1 
    
    # tier 1 downstream configuration
    t_rnacen__ontology_terms >> [t_rnacen__rnc_taxonomy, t_rnacen__rnc_accessions, t_rnacen__rnc_rna_precomputed]
    t_rnacen__rfam_clans >> t_rnacen__rnc_taxonomy
    t_rnacen__rnc_release >> t_rnacen__rnc_rna_precomputed
    t_rnacen__rfam_models >> end
    
    # tier 2 downstream configuration
    t_rnacen__rnc_rna_precomputed >> t_rnacen__rnc_interactions
    t_rnacen__rnc_taxonomy >> end
    t_rnacen__rnc_accessions >> end
    
    # tier 3 downstream configuration
    t_rnacen__rnc_interactions >> end
