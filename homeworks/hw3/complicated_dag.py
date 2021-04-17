from airflow.models import DAG
from airflow.operators.bash import BashOperator
from utils.settings import default_settings
import datetime as dt
from operators import *

# базовые аргументы DAG
args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}

# В контексте DAG'а зададим набор task'ок
# Объект-инстанс Operator'а - это и есть task
with DAG(**default_settings()) as dag:
    BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    ) >>  create_titanic_dataset() >> (pivot_titanic_dataset(), mean_fares_titanic_dataset()) >> BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is ds={{ ds }}"',
        dag=dag
    )