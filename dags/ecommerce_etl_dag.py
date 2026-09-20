from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def extract_orders_task(**kwargs):
    kwargs['ti'].xcom_push(key='orders', value='orders_data_extracted')
def extract_customers_task(**kwargs):
    kwargs['ti'].xcom_push(key='customers', value='customers_data_extracted')
def extract_products_task(**kwargs):
    kwargs['ti'].xcom_push(key='products', value='products_data_extracted')
def validate_data_task(**kwargs):
    pass
def transform_data_task(**kwargs):
    pass
def load_to_warehouse_task(**kwargs):
    pass
def run_dbt_models_task(**kwargs):
    pass
def send_alert_task(**kwargs):
    pass

with DAG('ecommerce_etl_dag', default_args=default_args, schedule_interval='@daily', start_date=datetime(2023, 1, 1), catchup=False) as dag:
    t_ext_orders = PythonOperator(task_id='extract_orders', python_callable=extract_orders_task)
    t_ext_cust = PythonOperator(task_id='extract_customers', python_callable=extract_customers_task)
    t_ext_prod = PythonOperator(task_id='extract_products', python_callable=extract_products_task)
    t_val = PythonOperator(task_id='validate_data', python_callable=validate_data_task)
    t_tr = PythonOperator(task_id='transform_data', python_callable=transform_data_task)
    t_load = PythonOperator(task_id='load_to_warehouse', python_callable=load_to_warehouse_task)
    t_dbt = PythonOperator(task_id='run_dbt_models', python_callable=run_dbt_models_task)
    t_alert = PythonOperator(task_id='send_alert', python_callable=send_alert_task)

    [t_ext_orders, t_ext_cust, t_ext_prod] >> t_val >> t_tr >> t_load >> t_dbt >> t_alert
