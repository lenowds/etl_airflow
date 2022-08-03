import requests
#from timedelta import Timedelta
from pathlib import Path
from datetime import datetime, date, timedelta
import pandas as pd
from sqlalchemy import create_engine
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator

Data = date.today().strftime("%Y-%m-%d")

# Path where it will be saving extract files from origin
PathDB          = './data/postgres/shippers/{}/{}.csv'

# URL connection to Azure Database for PostgreSQL server 
Con_StringFrom  = 'postgresql://northwind_user@lfsindicium:!thewindisblowing123@lfsindicium.postgres.database.azure.com:5432/northwind'

# URL connection Database destionation
Con_StringTo    = 'postgresql://northwind_user:thewindisblowing@localhost:5432/northwind'

default_args = {
    'owner': 'Leniel dos Santos',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def extract_shippers():
    FilePath = Path(PathDB.format(Data, 'shippers'))
    FilePath.parent.mkdir(parents=True, exist_ok=True)
    
    Db = create_engine(Con_StringFrom)
    Connection = Db.connect()

    df = pd.read_sql_query("SELECT * FROM SHIPPERS", 
                            Connection) 
    df.to_csv(FilePath, header=True, index=False)
    
    return FilePath

def load_shippers(ti):
    File = ti.xcom_pull(task_ids = 'extract_shippers') 
    df = pd.read_csv(File)

    Db = create_engine(Con_StringTo)
    Connection = Db.connect()

    df.to_sql('shippers', 
                con=Connection, 
                if_exists='append', 
                index=False)


with DAG('shippers',
         start_date = datetime(2022,7,30),
         max_active_runs = 1,
         schedule_interval = '0 0 * * *',
         default_args = default_args,
         catchup = False
        )as dag:

        extract_shippers = PythonOperator(
            task_id = 'extract_shippers',
            python_callable = extract_shippers  
        )

        load_shippers = PythonOperator(
            task_id = 'load_shippers',
            python_callable = load_shippers 
        )

        extract_shippers >> load_shippers