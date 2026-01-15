from airflow.decorators import dag, task
from datetime import datetime
from airflow.sensors.base import PokeReturnValue
import requests
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from minio import Minio
import json
from io import BytesIO
from airflow.providers.docker.operators.docker import DockerOperator

@dag(
    start_date=datetime(2025, 1, 1),
    schedule='0 0 * * *', 
    catchup=False,
    tags=['crypto_market'],
)
def crypto_market_dag():
    
    @task.sensor(poke_interval=10, timeout=300)
    def check_api_availability() -> PokeReturnValue:
        """Check if CoinGecko API is available"""
        http_hook = HttpHook(method='GET', http_conn_id='coingecko_api')
        response = http_hook.run(endpoint='/api/v3/ping')
        
        if response.status_code == 200:
            print("API is available!")
            return PokeReturnValue(is_done=True, xcom_value=response.json())
        else:
            print(f"API returned {response.status_code}, retrying...")
            return PokeReturnValue(is_done=False)
    

    @task
    def fetch_top3_blockchain() -> list:
        http_hook = HttpHook(method='GET', http_conn_id='coingecko_api')
        response = http_hook.run(endpoint='/api/v3/coins/markets',
        data={
            'vs_currency': 'usd',
            'order': 'market_cap_desc',
            'per_page': 3,
            'page': 1,
            'sparkline': False,
            'price_change_percentage': '24h'
        } )
        if response.status_code == 200:
            crypto_data = response.json()
            print("Successfully Fetch Top 3 Blockchain")
            return crypto_data
        else:
            print(f"Failed to fetch data: {response.status_code}")
            raise Exception("Failed to fetch data")
    
    @task
    def store_data(crypto_data):
        minio = BaseHook.get_connection('minio')
        client = Minio(
            endpoint=minio.extra_dejson['endpoint_url'].split('//')[1],
            access_key=minio.login,
            secret_key=minio.password,
            secure=False
        )
        bucket_name = 'crypto-market'
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)

        json_data = json.dumps(crypto_data, indent=2)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')  
        filename = f'crypto_market_{timestamp}.json'
        client.put_object(
            bucket_name = bucket_name,
            object_name = filename,
            data = BytesIO(json_data.encode('utf-8')),
            length = len(json_data),
            content_type = 'application/json'
        )
        print(f"uploaded to minio: {filename}")
        return filename

    @task
    def transform_data(filename : str):
        docker_op = DockerOperator(
         task_id = 'spark_transform',
         image = 'airflow/stock-app',
         docker_url = 'tcp://docker-proxy:2375',
         network_mode='container:spark-master',
         auto_remove='success',
         environment={
            'SPARK_APPLICATION_ARGS': f'crypto-market/{filename}'
            }   
        )
        result = docker_op.execute(context={})
        return f'crypto-market/transformed_{filename}'

    api_check = check_api_availability()
    crypto_data = fetch_top3_blockchain()
    stored_data = store_data(crypto_data)
    transformed_data = transform_data(stored_data)

    api_check >> crypto_data >> stored_data >> transformed_data

crypto_market_dag()