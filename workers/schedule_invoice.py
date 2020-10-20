import json
from datetime import timedelta

from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('invoice_http_operator', default_args=default_args, tags=['purchase order'], start_date=days_ago(2))

dag.doc_md = __doc__

# task_post_op, task_get_op and task_put_op are examples of tasks created by instantiating operators
# [START howto_operator_http_task_post_op]
task_post_op = SimpleHttpOperator(
    task_id='post_op',
    endpoint='https://stateset.network:8080/api/stateset/createInvoice',
    data=json.dumps({"priority": 5}),
    headers={"Content-Type": "application/json"},
    response_check=lambda response: response.json()['json']['priority'] == 5,
    dag=dag,
)
# [END howto_operator_http_task_post_op]
# [START howto_operator_http_task_post_op_formenc]
task_post_op_formenc = SimpleHttpOperator(
    task_id='post_op_formenc',
    endpoint='https://stateset.network:8080/api/stateset/createInvoice',
    data="name=Joe",
    headers={"Content-Type": "application/x-www-form-urlencoded"},
    dag=dag,
)
# [END howto_operator_http_task_post_op_formenc]
# [START howto_operator_http_task_get_op]
task_get_op = SimpleHttpOperator(
    task_id='get_op',
    method='GET',
    endpoint='get',
    data={"param1": "value1", "param2": "value2"},
    headers={},
    dag=dag,
)
# [END howto_operator_http_task_get_op]
# [START howto_operator_http_task_get_op_response_filter]
task_get_op_response_filter = SimpleHttpOperator(
    task_id='get_op_response_filter',
    method='GET',
    endpoint='get',
    response_filter=lambda response: response.json()['nested']['property'],
    dag=dag,
)
# [END howto_operator_http_task_del_op]
# [START howto_operator_http_http_sensor_check]
task_http_sensor_check = HttpSensor(
    task_id='http_sensor_check',
    http_conn_id='http_default',
    endpoint='',
    request_params={},
    response_check=lambda response: "httpbin" in response.text,
    poke_interval=5,
    dag=dag,
)
# [END howto_operator_http_http_sensor_check]
task_http_sensor_check >> task_post_op >> task_get_op >> task_get_op_response_filter
task_get_op_response_filter >> task_put_op >> task_del_op >> task_post_op_formenc