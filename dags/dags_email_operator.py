from airflow import DAG
import pendulum
import datetime
from airflow.operators.email import EmailOperator

with DAG(
    dag_id="dags_email_operator",
    schedule="0 8 1 * *",
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    send_email_task = EmailOperator(
        task_id= 'send_email_task',
        to='akfktl328@naver.com',
        subject="작업 진행 현황 공지",
        html_content="Airflow 작업을 완료했습니다."
    )
