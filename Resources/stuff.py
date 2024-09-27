def decompress_stream(self, api_url):
    """Stream and decompress data from the API without loading it all into memory."""
    try:
        logging.info(f"Fetching data from API: {api_url}")
        headers = self.get_auth_header(client_id=self._client_id, resource=self._resource)
        response = requests.get(api_url, headers=headers, stream=True)

        if response.status_code == 200:
            logging.info("Decompressing data stream")
            with gzip.GzipFile(fileobj=response.raw, mode='rb') as gz:
                for chunk in iter(lambda: gz.read(1024 * 1024 * 50), b''):
                    if chunk:
                        yield io.BytesIO(chunk)

    except requests.exceptions.RequestException as e:
        logging.error(f"Request failed: {e}")
        raise


def to_parquet_stream(self, buffer, parquet_writer, chunk_size=30000):
    """Convert CSV data chunks from a buffer to Parquet format incrementally."""
    for chunk in pd.read_csv(buffer, chunksize=chunk_size, low_memory=False, dtype=object):
        chunk.fillna('')  # Handle NaN values
        table = pa.Table.from_pandas(chunk, preserve_index=False)
        parquet_writer.write_table(table)


def fetch_convert_upload_to_global(self, table_name, month):
    """Fetch, convert to Parquet, and upload to S3 in a memory-efficient way."""
    logger.info(f"Starting upload process for {table_name} for month {month}")
    endpoint = self._url + self._mars_endpoint.replace('{month}', f'{month}')
    key = f"{self._bucket_root}/{table_name}/mars-{month}-{datetime.now().strftime('%Y%m%d%H%M%S')}.parquet"

    # Prepare Parquet writer and buffer
    parquet_buffer = io.BytesIO()
    parquet_writer = None

    try:
        for buffer in self.decompress_stream(endpoint):
            if parquet_writer is None:
                # Initialize the Parquet writer using the schema from the first chunk
                first_chunk = pd.read_csv(buffer, chunksize=1, low_memory=False, dtype=object)
                table = pa.Table.from_pandas(next(first_chunk), preserve_index=False)
                parquet_writer = pq.ParquetWriter(parquet_buffer, table.schema)
                # Write the first chunk to start the parquet file
                parquet_buffer.seek(0)

            # Convert and write the chunks to Parquet format
            self.to_parquet_stream(buffer, parquet_writer)

        # Finalize the Parquet file
        if parquet_writer:
            parquet_writer.close()

        # Seek to start of buffer for S3 upload
        parquet_buffer.seek(0)

        # Upload to S3
        file_size = parquet_buffer.getbuffer().nbytes
        logger.info(f"Starting upload to S3 bucket: key: {key}, size: {file_size / 1024 ** 3:.2f} GB")
        self._s3.upload_fileobj(parquet_buffer, key)
        logger.info(f"Uploaded file {key} successfully")

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise

    finally:
        # Clean up resources
        if parquet_writer:
            parquet_writer.close()
        del parquet_buffer
