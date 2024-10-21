from airflow.utils.edgemodifier import Label
from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable
import sqlite3
import pandas as pd
import os

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

db_path = os.path.join(os.getcwd(), 'data', 'Northwind_small.sqlite')

def read_order():
    try:
        with sqlite3.connect(db_path) as con:
            df_order = pd.read_sql_query('SELECT * FROM "Order"', con)
                    
            df_order.to_csv("output_orders.csv", index=False)
                                  
    except Exception as e:
        print(f'Ocorreu um erro: {e}')
            
def read_order_detail():
    order_tb = pd.read_csv("output_orders.csv", quotechar='"', sep=",")
    order_city = order_tb[order_tb["ShipCity"] == "Rio de Janeiro"]
    try:
        with sqlite3.connect(db_path) as con:
            order_detail = pd.read_sql_query("SELECT * FROM OrderDetail", con)
                
        join_tb = pd.merge(order_city, order_detail, left_on="Id", right_on="OrderId", how = "inner")
                     
        with open("count.txt", "w") as count:
            count.write(str(join_tb['Quantity'].sum()))
            
    except Exception as e:
        print(f'Ocorreu um erro: {e}')
            
## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
    """
   
    read_table_sqlite = PythonOperator(
        task_id='read_table_sqlite',
        python_callable=read_order,
        provide_context=True
    )
    
    read_table_csv = PythonOperator(
        task_id='read_table_csv',
        python_callable=read_order_detail,
        provide_context=True
    )
    
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    read_table_sqlite >> read_table_csv >> export_final_output