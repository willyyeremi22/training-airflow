from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pendulum


def debug_sys_path():
    import sys
    from collections import Counter

    print("=== sys.path ===")
    for p in sys.path:
        print(p)

    print("\n=== DUPLICATES ===")
    for p, n in Counter(sys.path).items():
        if n > 1:
            print(f"{p} -> {n}x")


with DAG(
    dag_id="check_path",
    description='check path',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=2,
    max_active_tasks=2,
    orientation="TB",
    tags=["latihan"]
) as dag:
    start = EmptyOperator(task_id="start")

    t_check_path = PythonOperator(task_id="check_path",python_callable=debug_sys_path)

    end = EmptyOperator(task_id="end")

    start >> t_check_path >> end