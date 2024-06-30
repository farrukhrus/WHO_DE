from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from load_dim import (truncate_dimensions, fetch_dimensions,
                      fetch_and_produce_dimension_values,
                      consume_and_load_dimension_values, 
                      load_dimensions_to_clickhouse)
from load_indicator import (truncate_indicators, load_indicators_to_clickhouse,
                            fetch_indicators, fetch_and_load_indicator_values)


args = {
    "owner": "user",
    'email': ['student@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
    'who_dim_data_loader',
    default_args=args,
    description='Load WHO dim-data into DIMENSIONS table',
    catchup=False,
    schedule_interval='@monthly',
    start_date=datetime(2024, 1, 1),
) as dag:
    truncate_table_task = PythonOperator(
        task_id='truncate_table',
        provide_context=True,
        python_callable=truncate_dimensions,
        dag=dag,
    )

    fetch_dimensions_task = PythonOperator(
        task_id='fetch_dimensions',
        provide_context=True,
        python_callable=fetch_dimensions,
        dag=dag,
    )

    load_dimensions_to_clickhouse = PythonOperator(
        task_id='load_dimensions_to_clickhouse',
        provide_context=True,
        python_callable=load_dimensions_to_clickhouse,
        dag=dag,
    )

    fetch_and_produce_dimension_values = PythonOperator(
        task_id='fetch_and_produce_dimension_values',
        provide_context=True,
        python_callable=fetch_and_produce_dimension_values,
        dag=dag,
    )

    consume_and_load_dimension_values = PythonOperator(
        task_id='consume_and_load_dimension_values',
        provide_context=True,
        python_callable=consume_and_load_dimension_values,
        dag=dag,
    )

    truncate_indicator_table_task = PythonOperator(
        task_id='truncate_indicator_table',
        provide_context=True,
        python_callable=truncate_indicators,
        dag=dag,
    )

    fetch_indicators_task = PythonOperator(
        task_id='fetch_indicators',
        provide_context=True,
        python_callable=fetch_indicators,
        dag=dag,
    )

    load_indicators_to_clickhouse = PythonOperator(
        task_id='load_indicators_to_clickhouse',
        provide_context=True,
        python_callable=load_indicators_to_clickhouse,
        dag=dag,
    )

    fetch_and_load_indicator_values = PythonOperator(
        task_id='fetch_and_load_indicator_values',
        provide_context=True,
        python_callable=fetch_and_load_indicator_values,
        dag=dag,
    )

    (truncate_table_task >> fetch_dimensions_task >> load_dimensions_to_clickhouse >> fetch_and_produce_dimension_values >> consume_and_load_dimension_values)

    (truncate_indicator_table_task >> fetch_indicators_task >> load_indicators_to_clickhouse >> fetch_and_load_indicator_values)
