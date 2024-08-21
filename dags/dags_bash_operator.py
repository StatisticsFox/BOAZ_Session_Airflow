from airflow import DAG
from airflow.operators.bash import BashOperator

import datetime
import pendulum # datetime 라이브러리를 더 편하게 쓸 수 있게 해주는 함수

with DAG(
    dag_id="dags_bash_operator", # dags 이름 화면에서 보이는 값 파일 명과 똑같이 하는게 신상에 좋다.
    schedule="0 0 * * *", # cron 스케줄 "분 시 일 월 요일"
    start_date=pendulum.datetime(2021, 1, 1, tz="Asia/Seoul"), # dags가 언제 도는지 나타냄
    catchup=False, # start_date변수와 함께 보는 변수, True면 현 시점 과 start_date 사이의 모든 구간을 한꺼번에 dags가 돌음
) as dag:
     bash_t1 = BashOperator( # <- 객체 명을 의미 task ID와 똑같이 하도록 하자
        task_id="bash_t1", # <- task 이름, 화면에서 보이는 값이기 때문에 객체 명과 똑같이 하는게 신상에 좋다.
        bash_command="echo Whoami",
    )
     
     bash_t2 = BashOperator( 
        task_id="bash_t2", 
        bash_command="echo $HOSTNAME",
    )
     
     bash_t1 >> bash_t2
