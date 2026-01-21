from airflow.decorators import dag, task
from datetime import datetime
from airflow.sensors.base import PokeReturnValue
import requests
from airflow.providers.http.hooks.http import HttpHook
from airflow.hooks.base import BaseHook
from minio import Minio
import json
from io import BytesIO
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.docker.operators.docker import DockerOperator
import csv
from io import StringIO

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
        return f'transformed_{filename}'
    
    @task
    def load_data(spark_output_folder: str):
        # 1. Setup MinIO Connection
        minio_hook = BaseHook.get_connection('minio')
        client = Minio(
            endpoint=minio_hook.extra_dejson['endpoint_url'].split('//')[1],
            access_key=minio_hook.login,
            secret_key=minio_hook.password,
            secure=False
        )
        bucket_name = 'crypto-market'

        # 2. Find the CSV file inside the Spark output folder
        print(f"Looking for CSV files in: {spark_output_folder}")
        objects = client.list_objects(bucket_name, prefix=spark_output_folder, recursive=True)
        csv_file_path = None
        for obj in objects:
            if obj.object_name.endswith('.csv'):
                csv_file_path = obj.object_name
                print(f"Found CSV file: {csv_file_path}")
                break
        
        if not csv_file_path:
            raise Exception(f"No CSV file found in {spark_output_folder}")

        # 3. Read the CSV content from MinIO
        response = client.get_object(bucket_name, csv_file_path)
        csv_content = response.read().decode('utf-8')
        
        # 4. Prepare Data for Postgres (Mapping Columns)
        rows_to_insert = []
        csv_reader = csv.DictReader(StringIO(csv_content))
        
        for row in csv_reader:
            # Map Spark CSV fields to Postgres Table fields
            # Handle potential empty strings for numeric fields
            def safe_float(val): return float(val) if val else None
            def safe_int(val): return int(float(val)) if val else None # handle '100.0' strings
            
            cleaned_row = (
                row['id'],                  # coin_id
                row['symbol'],              # symbol
                row['name'],                # name
                safe_float(row['current_price']), # price_usd
                safe_int(row['market_cap']),      # market_cap
                safe_int(row['total_volume']),    # volume_24h
                safe_float(row['price_change_24h']),      # price_change_24h
                safe_float(row['price_change_percentage_24h']), # price_change_pct_24h
                safe_float(row['high_24h']),      # high_24h
                safe_float(row['low_24h']),       # low_24h
                row['last_updated'],        # last_updated
                row['processed_at']         # extracted_at
            )
            rows_to_insert.append(cleaned_row)

        # 5. Insert into Postgres using PostgresHook
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        target_fields = [
            'coin_id', 'symbol', 'name', 'price_usd', 'market_cap', 'volume_24h', 
            'price_change_24h', 'price_change_pct_24h', 'high_24h', 'low_24h', 
            'last_updated', 'extracted_at'
        ]
        
        pg_hook.insert_rows(
            table='crypto_prices',
            rows=rows_to_insert,
            target_fields=target_fields
        )
        
        print(f"Successfully loaded {len(rows_to_insert)} rows into Postgres!")

    api_check = check_api_availability()
    crypto_data = fetch_top3_blockchain()
    stored_data = store_data(crypto_data)
    transformed_data = transform_data(stored_data)
    loaded_data = load_data(transformed_data)
   
    api_check >> crypto_data >> stored_data >> transformed_data >> loaded_data

crypto_market_dag()