import requests
#from timedelta import Timedelta
from pathlib import Path
from datetime import datetime, date, timedelta
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

Data = date.today().strftime("%Y-%m-%d")

# Azure Blob Storage
URL = 'https://lfsindicium.blob.core.windows.net/indicium-ed/order_details.csv'

# Caminho onde serão armazenados os arquivos extraídos da origem
PathDB      = './data/postgres/{}/{}/{}.csv'
PathCSV     = './data/csv/{}/{}.csv'
Con_String  = 'postgresql://northwind_user:thewindisblowing@localhost:5432/northwind'

default_args = {
    'owner': 'Leniel dos Santos',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_orders_details():
    FilePath = Path(PathCSV.format(Data, 'orders_details'))
    FilePath.parent.mkdir(parents=True, exist_ok=True)
    
    df = pd.read_csv(URL)
    df.to_csv(FilePath, header=True, index=False)
    return FilePath

def load_orders_details(ti):
    File = ti.xcom_pull(task_ids = 'extract_orders_details') 
    df = pd.read_csv(File)

    Db = create_engine(Con_String)
    Connection = Db.connect()

    df.to_sql('order_details', 
                con=Connection, 
                if_exists='append', 
                index=False)


with DAG('orders_details',
         start_date = datetime(2022,7,30),
         max_active_runs = 1,
         schedule_interval = '0 0 * * *',
         default_args = default_args,
         catchup = False
        )as dag:

        extract_orders_details = PythonOperator(
            task_id = 'extract_orders_details',
            python_callable = extract_orders_details  
        )

        load_orders_details = PythonOperator(
            task_id = 'load_orders_details',
            python_callable = load_orders_details 
        )

        extract_orders_details >> load_orders_details