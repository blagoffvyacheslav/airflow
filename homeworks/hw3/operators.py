from airflow.operators.python import PythonOperator
from utils.deco import python_operator
from airflow.models import Variable
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine('postgresql://airflow:airflow@localhost:5432/airflow_ex')
CALC1 = Variable.get("calc1")
CALC2 = Variable.get("calc2")

@python_operator()
def create_titanic_dataset(**context):
    url = 'https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv'
    df = pd.read_csv(url)
    context['task_instance'].xcom_push('df', df.to_dict('records'))

@python_operator()
def pivot_titanic_dataset(**context):
    titanic_df = pd.DataFrame.from_dict(context['task_instance'].xcom_pull(task_ids="create_titanic_dataset", key='df'))
    df = titanic_df.pivot_table(index=['Sex'],
                                columns=['Pclass'],
                                values='Name',
                                aggfunc='count').reset_index()
    df.to_sql(CALC1, con=engine)

@python_operator()
def mean_fares_titanic_dataset(**context):
    titanic_df = pd.DataFrame.from_dict(context['task_instance'].xcom_pull(task_ids="create_titanic_dataset", key='df'))
    df = titanic_df.groupby('Pclass', as_index=False)['Fare'].mean()
    df.to_sql(CALC2, con=engine)