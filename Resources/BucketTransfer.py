import boto3
import io
import zipfile
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
CHUNK_SIZE = 16 * 1024 * 1024  # 16 MB
MAX_WORKERS = 20  # Number of concurrent threads for downloading and zipping
UPLOAD_PART_SIZE = 16 * 1024 * 1024  # 16 MB part size for multipart upload
UPLOAD_MAX_WORKERS = 10  # Number of concurrent threads for uploading
MAX_RETRIES = 3  # Maximum number of retries for uploads

def stream_and_zip_object(source_key):
    """
    Downloads an object from an S3 bucket, reads it in chunks, and returns the object key, file content, and file size.

    Args:
        source_key (str): The key of the object to download from the S3 bucket.

    Returns:
        tuple: A tuple containing the source key, file content as bytes, and file size in bytes.

    Raises:
        BotoCoreError: If there is an error with the BotoCore library.
        ClientError: If there is an error with the S3 client.

    """
    try:
        obj = s3_client.get_object(Bucket=SOURCE_BUCKET, Key=source_key)
        obj_body = obj['Body']
        file_size = obj['ContentLength']
        file_buffer = io.BytesIO()

        # Read the object in chunks and write to the file buffer
        for chunk in iter(lambda: obj_body.read(CHUNK_SIZE), b''):
            file_buffer.write(chunk)

        file_buffer.seek(0)
        return source_key, file_buffer.read(), file_size
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error downloading {source_key}: {e}")
        return None, None, None

def upload_part_with_retry(multipart_upload_id, part_number, part_data, key):
    """
    Uploads a part of a file to the destination bucket with retry logic.

    Args:
        multipart_upload_id (str): The ID of the multipart upload.
        part_number (int): The part number of the part being uploaded.
        part_data (bytes): The data of the part being uploaded.
        key (str): The key of the object in the destination bucket.

    Returns:
        dict or None: A dictionary containing the part number and ETag of the uploaded part,
        or None if the upload fails after the maximum number of retries.
    """
    for attempt in range(MAX_RETRIES):
        try:
            part_response = s3_client.upload_part(
                Body=part_data,
                Bucket=DESTINATION_BUCKET,
                Key=key,
                UploadId=multipart_upload_id,
                PartNumber=part_number
            )
            logger.info(f"Uploaded part {part_number} for {key} on attempt {attempt + 1}")
            return {'PartNumber': part_number, 'ETag': part_response['ETag']}
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error uploading part {part_number} for {key} on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Failed to upload part {part_number} for {key} after {MAX_RETRIES} attempts")
    return None

def complete_multipart_upload_with_retry(multipart_upload_id, key, parts):
    """
    Completes a multipart upload with retry logic.

    Args:
        multipart_upload_id (str): The ID of the multipart upload.
        key (str): The key of the object in the destination bucket.
        parts (list): A list of parts to be uploaded.

    Raises:
        BotoCoreError: If there is an error during the upload process.
        ClientError: If there is an error with the S3 client.

    Returns:
        None
    """
    for attempt in range(MAX_RETRIES):
        try:
            s3_client.complete_multipart_upload(
                Bucket=DESTINATION_BUCKET,
                Key=key,
                MultipartUpload={'Parts': parts},
                UploadId=multipart_upload_id
            )
            logger.info(f"Completed upload for {key} on attempt {attempt + 1}")
            return
        except (BotoCoreError, ClientError) as e:
            logger.error(f"Error completing upload for {key} on attempt {attempt + 1}: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logger.error(f"Failed to complete upload for {key} after {MAX_RETRIES} attempts")
                raise

def stream_and_zip_objects():
    """
    Streams objects from a source bucket, zips them, and uploads the zip file and a manifest file to a destination bucket.

    This function performs the following steps:
    1. Generates a timestamp for the snapshot.
    2. Creates a directory name and zip file name based on the timestamp.
    3. Initializes a zip buffer to store the zipped objects.
    4. Initializes an empty list to store the manifest content.
    5. Retrieves objects from the source bucket using pagination.
    6. Streams and zips each object in parallel using a thread pool executor.
    7. Writes the zipped object to the zip buffer and adds the object's details to the manifest content.
    8. Seeks to the beginning of the zip buffer.
    9. Starts a multipart upload for the zip file in the destination bucket.
    10. Reads and uploads each part of the zip file in parallel using a thread pool executor.
    11. Completes the multipart upload by combining the uploaded parts.
    12. If any error occurs during the multipart upload, aborts the upload.
    13. Uploads the manifest file to the destination bucket.
    14. Logs the completion of the snapshot.

    Note: This function assumes the existence of the following variables:
    - SOURCE_BUCKET: The name of the source bucket.
    - DESTINATION_BUCKET: The name of the destination bucket.
    - MAX_WORKERS: The maximum number of workers for streaming and zipping objects.
    - UPLOAD_MAX_WORKERS: The maximum number of workers for uploading parts of the zip file.
    - UPLOAD_PART_SIZE: The size of each part of the zip file for uploading.

    Returns:
    None
    """
    timestamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')
    directory_name = f'{DESTINATION_BUCKET}-Snapshot-{timestamp}'
    zip_filename = f'{directory_name}/snapshot-{timestamp}.zip'
    manifest_filename = f'{directory_name}/manifest-{timestamp}.txt'
    zip_buffer = io.BytesIO()

    manifest_content = []

    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        paginator = s3_client.get_paginator('list_objects_v2')
        for page in paginator.paginate(Bucket=SOURCE_BUCKET):
            if 'Contents' not in page:
                logger.info("No objects found in the source bucket.")
                return

            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = [executor.submit(stream_and_zip_object, obj['Key']) for obj in page['Contents']]

                for future in as_completed(futures):
                    source_key, obj_data, file_size = future.result()
                    if source_key and obj_data:
                        zipf.writestr(source_key, obj_data)
                        manifest_content.append(f'{source_key}\t{file_size}\n')

    zip_buffer.seek(0)

    # Start multipart upload
    multipart_upload_response = s3_client.create_multipart_upload(Bucket=DESTINATION_BUCKET, Key=zip_filename)
    multipart_upload_id = multipart_upload_response['UploadId']

    parts = []
    part_number = 1
    upload_futures = []

    with ThreadPoolExecutor(max_workers=UPLOAD_MAX_WORKERS) as upload_executor:
        while True:
            part_data = zip_buffer.read(UPLOAD_PART_SIZE)
            if not part_data:
                break
            future = upload_executor.submit(upload_part_with_retry, multipart_upload_id, part_number, part_data, zip_filename)
            upload_futures.append(future)
            part_number += 1

        for future in as_completed(upload_futures):
            result = future.result()
            if result:
                parts.append(result)

    if parts:
        parts.sort(key=lambda x: x['PartNumber'])
        try:
            complete_multipart_upload_with_retry(multipart_upload_id, zip_filename, parts)
        except (BotoCoreError, ClientError):
            logger.error(f"Failed to complete multipart upload for {zip_filename}, aborting.")
            s3_client.abort_multipart_upload(Bucket=DESTINATION_BUCKET, Key=zip_filename, UploadId=multipart_upload_id)
    else:
        logger.error(f"No parts uploaded for {zip_filename}, aborting multipart upload.")
        s3_client.abort_multipart_upload(Bucket=DESTINATION_BUCKET, Key=zip_filename, UploadId=multipart_upload_id)

    # Upload the manifest file
    manifest_data = ''.join(manifest_content).encode('utf-8')
    manifest_buffer = io.BytesIO(manifest_data)
    try:
        s3_client.upload_fileobj(manifest_buffer, DESTINATION_BUCKET, manifest_filename)
        logger.info(f'Uploaded manifest file {manifest_filename}')
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Error uploading manifest file {manifest_filename}: {e}")

    logger.info('Snapshot completed.')

if __name__ == '__main__':
    stream_and_zip_objects()