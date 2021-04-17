from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from sqlalchemy import create_engine
from airflow.models import Variable

import datetime as dt
import pandas as pd
engine = create_engine('postgresql://airflow:airflow@localhost:5432/airflow_ex')
CALC1 = Variable.get("calc1")
CALC2 = Variable.get("calc2")

# базовые аргументы DAG
args = {
    'owner': 'airflow',  # Информация о владельце DAG
    'retries': 1,  # Количество повторений в случае неудач
    'retry_delay': dt.timedelta(minutes=1),  # Пауза между повторами
    'depends_on_past': False,  # Запуск DAG зависит ли от успешности окончания предыдущего запуска по расписанию
}

@dag(default_args=args, start_date=dt.datetime(2020, 12, 23))
def flow():
    @task
    def start():
        BashOperator(
            task_id='start',
            bash_command='echo "Here we start! Info: run_id={{ run_id }} | dag_run={{ dag_run }}"',
        )
        return 1

    @task
    def create_titanic_dataset(status):
        if status:
            url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
            df = pd.read_csv(url)
            return df.to_dict('records')
        else:
            raise NotImplementedError('Dag error')

    @task
    def pivot_titanic_dataset(res):
        titanic_df = pd.DataFrame.from_dict(
            res)
        df = titanic_df.pivot_table(index=['Sex'],
                                    columns=['Pclass'],
                                    values='Name',
                                    aggfunc='count').reset_index()
        df.to_sql(CALC1, con=engine)

    @task
    def mean_fares_titanic_dataset(res):
        titanic_df = pd.DataFrame.from_dict(
            res)
        df = titanic_df.groupby('Pclass', as_index=False)['Fare'].mean()
        df.to_sql(CALC2, con=engine)

    @task
    def finish(res1, res2):
        BashOperator(
                task_id='finish',
                bash_command='echo "Pipeline finished! Execution date is ds={{ ds }}"',
        )

    initial = start()
    data = create_titanic_dataset(initial)
    process1 = pivot_titanic_dataset(data)
    process2 = mean_fares_titanic_dataset(data)
    finish(process1, process2)

flow_dag = flow()