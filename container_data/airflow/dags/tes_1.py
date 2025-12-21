from airflow.models.dag import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.edgemodifier import Label
import pendulum


with DAG(
    dag_id="tes_1",
    description='latihan 1',
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=2,
    max_active_tasks=2,
    orientation="TB",
    tags=["latihan"]
) as dag:
    start = EmptyOperator(task_id="start")

    with TaskGroup("section_1", tooltip="Tasks for section_1") as section_1:
        task_1 = EmptyOperator(task_id="task_1")
        task_2 = EmptyOperator(task_id="task_2")

    with TaskGroup("section_2", tooltip="Tasks for section_2") as section_2:
        task_3 = EmptyOperator(task_id="task_3")
        task_4 = EmptyOperator(task_id="task_4")
        task_5 = EmptyOperator(task_id="task_5")
        task_6 = EmptyOperator(task_id="task_6")

    with TaskGroup("section_3", tooltip="Tasks for section_3") as section_3:
        task_7 = EmptyOperator(task_id="task_7")
        task_8 = EmptyOperator(task_id="task_8")
        task_9 = EmptyOperator(task_id="task_9")

    end = EmptyOperator(task_id="end")

    start >> Label("Starting DAG") >> section_1
    
    # section_1 downstream configuration
    task_1 >> [task_3, task_5]
    task_2 >> [task_4, task_6]
    
    # section_2 downstream configuration
    [task_5,task_3] >> task_7
    task_5 >> task_9
    task_6 >> task_8
    task_4 >> end
    
    # section_3 downstream configuration
    section_3 >> Label("Ending DAG") >> end