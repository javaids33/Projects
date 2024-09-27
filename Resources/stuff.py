 def upload_object(self, table_name, month):
        logger = logging.getLogger(__name__)
        logger.info(f"Starting upload process for {table_name} for month {month}")
        endpoint = self._url + self._mars_endpoint.replace('{month}', f'{month}')
        headers = self.get_auth_header(client_id=self._client_id, resource=self._resource)
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        key = f"{self._bucket_root}/{table_name}/mars-{month}-{timestamp}.parquet"

        try:
            # Download and decompress data
            buffer = self.decompress(endpoint, headers)
            # Convert to Parquet format
            parquet_buffer = self.to_parquet(buffer)
            # Upload to S3
            file_size = parquet_buffer.getbuffer().nbytes
            logger.info(f"Starting upload to S3 bucket: key: {key}, size: {file_size / (1024 ** 3):.2f} GB")
            self._s3.upload_fileobj(parquet_buffer, self._bucket_name, key)
            logger.info(f"Uploaded file {key} successfully")
            del parquet_buffer
        except ClientError as err:
            logger.error(f"Error uploading to S3: {err.response['Error']['Message']}")
            raise ValueError("Error uploading Parquet file to S3.")
        except Exception as e:
            logger.error(f"An unexpected error occurred: {str(e)}")
            raise

    def decompress(self, api_url, headers):
        logger = logging.getLogger(__name__)
        try:
            logger.info(f"Fetching data from API: {api_url}")
            response = requests.get(api_url, headers=headers, stream=True, timeout=300)
            response.raise_for_status()

            # Decompress the gzipped content on the fly using a streaming approach
            def generate_lines():
                with gzip.GzipFile(fileobj=response.raw, mode='rb') as gz:
                    for line in gz:
                        yield line

            logger.info("Data fetched and decompressed successfully")
            return generate_lines()

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            raise

    def to_parquet(self, lines_generator):
        logger = logging.getLogger(__name__)
        parquet_buffer = io.BytesIO()
        logger.info("Starting conversion of CSV data to Parquet format")

        # Use pyarrow to read CSV lines and write to Parquet
        read_options = pc.ReadOptions(use_threads=True)
        parse_options = pc.ParseOptions(delimiter=',')
        convert_options = pc.ConvertOptions(strings_can_be_null=True)

        table = pc.read_csv(
            lines_generator,
            read_options=read_options,
            parse_options=parse_options,
            convert_options=convert_options
        )

        pq.write_table(table, parquet_buffer, compression='snappy')
        parquet_buffer.seek(0)
        logger.info("Data converted to Parquet format successfully")
        return parquet_buffer