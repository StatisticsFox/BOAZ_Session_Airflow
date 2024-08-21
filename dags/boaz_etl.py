from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import dag, task
import requests
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# DAG 설정
@dag(
    dag_id="boaz_etl",
    schedule_interval="@daily",
    start_date=datetime(2022, 6, 15),
    default_args={
        'retries': 3,
        'retry_delay': timedelta(minutes=3),
    },
    tags=["BOAZ"],
    catchup=False
)
def boaz_etl():
    
    # 데이터 추출
    @task
    def extract():
        url = "http://openapi.seoul.go.kr:8088/6474414554616b6636307176464943/json/bikeList/1/1000"
        response = requests.get(url)
        response.raise_for_status()  # HTTP 에러 발생 시 예외를 일으킴
        return response.json()

    # 데이터 변환
    @task
    def transform(data):
        # JSON 데이터에서 필요한 정보를 추출하여 데이터프레임으로 변환
        rows = data['rentBikeStatus']['row']
        df = pd.DataFrame(rows)
        return df

    # 데이터 적재
    @task
    def load(df):
        table = pa.Table.from_pandas(df)
        pq.write_table(table, '/tmp/job_search_result.parquet')

    # 결과 출력 (10개만 출력)
    @task
    def output(df):
        print(df.head(10))
    
    # Task 연결
    extracted_data = extract()
    transformed_data = transform(extracted_data)
    load_task = load(transformed_data)
    output_task = output(transformed_data)

    load_task >> output_task

# DAG 인스턴스 생성
dag_instance = boaz_etl()
