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
PathDB          = './data/postgres/employees/{}/{}.csv'

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

def extract_employees():
    FilePath = Path(PathDB.format(Data, 'employees'))
    FilePath.parent.mkdir(parents=True, exist_ok=True)
    
    Db = create_engine(Con_StringFrom)
    Connection = Db.connect()

    df = pd.read_sql_query("SELECT * FROM EMPLOYEES", 
                            Connection) 
    df.to_csv(FilePath, header=True, index=False)
    
    return FilePath

def load_employees(ti):
    File = ti.xcom_pull(task_ids = 'extract_employees') 
    df = pd.read_csv(File)

    Db = create_engine(Con_StringTo)
    Connection = Db.connect()

    df.to_sql('employees', 
                con=Connection, 
                if_exists='append', 
                index=False)


with DAG('employees',
         start_date = datetime(2022,7,30),
         max_active_runs = 1,
         schedule_interval = '0 0 * * *',
         default_args = default_args,
         catchup = False
        )as dag:

        extract_employees = PythonOperator(
            task_id = 'extract_employees',
            python_callable = extract_employees  
        )

        load_employees = PythonOperator(
            task_id = 'load_employees',
            python_callable = load_employees 
        )

        extract_employees >> load_employees