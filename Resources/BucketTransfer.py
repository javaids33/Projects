import boto3
import io
import logging
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from botocore.exceptions import BotoCoreError, ClientError
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3_client = boto3.client('s3')

# Source and destination bucket names
SOURCE_BUCKET = 'your-source-bucket'
DESTINATION_BUCKET = 'your-neptune-bucket'
MAX_WORKERS = 20  # Number of concurrent threads for uploading
MAX_RETRIES = 3  # Maximum number of retries for uploads

def upload_file_with_retry(source_key, destination_key):
    """
    Uploads a file from the source bucket to the destination bucket with retry logic.

    Args:
        source_key (str): The key of the file in the source bucket.
        destination_key (str): The key of the file in the destination bucket.

    Returns:
        None

    Raises:
        BotoCoreError: If there is an error with the BotoCore library.
        ClientError: If there is an error with the S3 client.

    """
    for attempt in range(MAX_RETRIES):
        try:
            copy_source = {'Bucket': SOURCE_BUCKET, 'Key': source_key}
            s3_client.copy_object(CopySource=copy_source, Bucket=DESTINATION_BUCKET, Key=destination_key)
            logger.info(f"Uploaded {source_key} to {destination_key} on attempt {attempt + 1}")
            return
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error uploading {source_key} to {destination_key} on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Failed to upload {source_key} to {destination_key} after {MAX_RETRIES} attempts")

def upload_files_from_bucket():
    """
    Uploads files from the source bucket to the destination bucket.

    This function retrieves the objects from the source bucket using pagination,
    and uploads each object to the destination bucket in parallel using a thread pool.
    It also generates a manifest file containing the details of the uploaded files.

    Returns:
        None
    """
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    directory_name = f'{DESTINATION_BUCKET}-Snapshot-{timestamp}'
    manifest_filename = f'{directory_name}/manifest-{timestamp}.txt'
    manifest_content = []

    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=SOURCE_BUCKET):
        if 'Contents' not in page:
            logger.info("No objects found in the source bucket.")
            return

        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []
            for obj in page['Contents']:
                source_key = obj['Key']
                destination_key = f'{directory_name}/{source_key}'
                futures.append(executor.submit(upload_file_with_retry, source_key, destination_key))
                manifest_content.append(f'{source_key}\t{obj["Size"]}\n')

            for future in as_completed(futures):
                future.result()

    # Upload the manifest file
    manifest_data = ''.join(manifest_content).encode('utf-8')
    manifest_buffer = io.BytesIO(manifest_data)
    try:
        s3_client.upload_fileobj(manifest_buffer, DESTINATION_BUCKET, manifest_filename)
        logger.info(f'Uploaded manifest file {manifest_filename}')
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error uploading manifest file {manifest_filename}: {e}")

    logger.info('Direct push completed.')

if __name__ == '__main__':
    direct_push_files()