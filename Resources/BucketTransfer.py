from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import os
import hashlib
import logging
from botocore.config import Config
from botocore.exceptions import BotoCoreError, ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configure Boto3 client with a larger connection pool
boto3_config = Config(max_pool_connections=50)  # Increase pool size as needed
s3_client = boto3.client('s3', config=boto3_config)

# Source and destination bucket names
SOURCE_BUCKET = 'your-source-bucket'
DESTINATION_BUCKET = 'your-neptune-bucket'
CHUNK_SIZE = 8 * 1024 * 1024  # 8 MB
TEMP_DIR = '/tmp'  # Directory to store temporary files

def calculate_sha256(file_path, chunk_size=8192):
    sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(chunk_size), b""):
            sha256.update(chunk)
    return sha256.hexdigest()

def download_file(source_key, local_path):
    s3_client.download_file(SOURCE_BUCKET, source_key, local_path)

def upload_file(local_path, destination_key):
    file_checksum = calculate_sha256(local_path)
    s3_client.upload_file(
        Filename=local_path,
        Bucket=DESTINATION_BUCKET,
        Key=destination_key,
        ExtraArgs={'Metadata': {'sha256': file_checksum}}
    )
    logger.info(f"Uploaded {local_path} with checksum: {file_checksum}")

def verify_file_checksum(bucket, key, local_path):
    local_checksum = calculate_sha256(local_path)
    response = s3_client.head_object(Bucket=bucket, Key=key)
    s3_checksum = response['Metadata'].get('sha256', '')
    if local_checksum == s3_checksum:
        logger.info(f"Checksum verified for {key}.")
        return True
    else:
        logger.warning(f"Checksum mismatch for {key}. Local: {local_checksum}, S3: {s3_checksum}")
        return False

def process_file(source_key, destination_key, file_size):
    local_path = os.path.join(TEMP_DIR, os.path.basename(source_key))
    try:
        if file_size > 0:
            download_file(source_key, local_path)
            upload_file(local_path, destination_key)
            verify_file_checksum(DESTINATION_BUCKET, destination_key, local_path)
            if os.path.exists(local_path):
                os.remove(local_path)
            return f"Processed {source_key} successfully"
        else:
            return f"Skipped {source_key} as it has zero file size."
    except Exception as e:
        logger.error(f"Error processing {source_key}: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return f"Failed to process {source_key}"

def download_upload_verify():
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=SOURCE_BUCKET):
        if 'Contents' in page:
            for obj in page['Contents']:
                source_key = obj['Key']
                destination_key = f'{DESTINATION_BUCKET}-Snapshot/{source_key}'
                file_size = obj['Size']
                process_file(source_key, destination_key, file_size)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    's3_transfer_dag',
    default_args=default_args,
    description='A simple DAG to download, verify, and upload files to S3',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:
    
    transfer_task = PythonOperator(
        task_id='download_upload_verify',
        python_callable=download_upload_verify,
    )
