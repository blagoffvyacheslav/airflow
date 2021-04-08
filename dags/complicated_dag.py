import os
import datetime as dt
import pandas as pd
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from utils.settings import default_settings
from utils.deco import python_operator


# базовые аргументы DAG
args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'start_date': dt.datetime(2020, 12, 23),  # Время начала выполнения пайплайна
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}


def get_path(file_name):
    return os.path.join(os.path.expanduser('~'), file_name)

@python_operator()
def create_titanic_dataset():
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    df.to_csv(get_path('titanic.csv'), encoding='utf-8')

# def download_titanic_dataset():
#     url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
#     df = pd.read_csv(url)
#     df.to_csv(get_path('titanic.csv'), encoding='utf-8')

@python_operator()
def pivot_titanic_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_csv(get_path('titanic_pivot.csv'))


# def pivot_dataset():
#     titanic_df = pd.read_csv(get_path('titanic.csv'))
#     df = titanic_df.pivot_table(index=['Sex'],
#                                 columns=['Pclass'],
#                                 values='Name',
#                                 aggfunc='count').reset_index()
#     df.to_csv(get_path('titanic_pivot.csv'))

@python_operator()
def mean_fares_titanic_dataset():
    titanic_df = pd.read_csv(get_path('titanic.csv'))
    df = titanic_df.groupby('Pclass', as_index=False)['Fare'].mean()
    df.to_csv(get_path('titanic_mean_fares.csv'))

# def mean_fares_per_class():
#     titanic_df = pd.read_csv(get_path('titanic.csv'))
#     df = titanic_df.groupby('Pclass', as_index=False)['Fare'].mean()
#     df.to_csv(get_path('titanic_mean_fares.csv'))


# В контексте DAG'а зададим набор task'ок
# Объект-инстанс Operator'а - это и есть task
with DAG(**default_settings()) as dag:
    # BashOperator, выполняющий указанную bash-команду
    first_task = BashOperator(
        task_id='first_task',
        bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        dag=dag,
    )
    # Загрузка датасета
    create_titanic_dataset()
    # Чтение, преобразование и запись датасета
    pivot_titanic_dataset()
    #  Расчет средней арифметической цены за билет для каждого класса
    mean_fares_titanic_dataset()
    #  Уведомление об окончании расчета
    last_task = BashOperator(
        task_id='last_task',
        bash_command='echo "Pipeline finished! Execution date is ds={{ ds }}"',
        dag=dag
    )
    # Порядок выполнения тасок
    create_titanic_dataset >> (mean_fares_titanic_dataset, pivot_titanic_dataset)