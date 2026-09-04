from google.cloud import storage
from constants import DEFAULT_BUCKET_NAME
import json
import os
import Parser
from datetime import datetime

class GCSManager:
    def __init__(self, bucket_name=DEFAULT_BUCKET_NAME, prefix=None, siemplify=None, sa_key=None, workload_identity_email=None):
        """
        Initializes the GCSManager.
        
        Args:
            bucket_name (str): The name of the GCS bucket.
            prefix (str): Optional prefix/folder path within the bucket.
            siemplify: The Siemplify action/job instance for logging.
            sa_key (str): Optional Service Account JSON key string.
            workload_identity_email (str): Optional Workload Identity Email.
        """
        self.logger = siemplify.LOGGER if siemplify else None
        if not bucket_name or bucket_name == "None":
            bucket_name = DEFAULT_BUCKET_NAME
        self.bucket_name = bucket_name
        self.prefix = prefix
        
        # Authentication
        if sa_key and sa_key != 'None':
            import json
            try:
                creds_dict = json.loads(sa_key)
                self.client = storage.Client.from_service_account_info(creds_dict)
                if self.logger:
                    self.logger.info("Initialized GCS Client with Service Account Key.")
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Failed to initialize GCS Client with SA Key: {e}")
                raise
        elif workload_identity_email and workload_identity_email != 'None':
            from google.auth import impersonated_credentials
            import google.auth
            try:
                source_creds, project = google.auth.default()
                creds = impersonated_credentials.Credentials(
                    source_credentials=source_creds,
                    target_principal=workload_identity_email,
                    target_scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
                self.client = storage.Client(credentials=creds)
                if self.logger:
                    self.logger.info(f"Initialized GCS Client with Workload Identity (Impersonating: {workload_identity_email}).")
            except Exception as e:
                if self.logger:
                    self.logger.error(f"Failed to initialize GCS Client with Workload Identity: {e}")
                raise
        else:
            self.client = storage.Client()
            if self.logger:
                self.logger.info("Initialized GCS Client with Default Credentials.")

        self.bucket = self.client.bucket(self.bucket_name)
        self.parser = Parser
        self.new_base_time = datetime.now()
        
        if self.logger:
            self.logger.info(f"GCSManager initialized for bucket: {self.bucket_name}, prefix: {self.prefix}")
        self.logstory_timestamps = None

    def _get_logstory_timestamps(self):
        if self.logstory_timestamps is not None:
            return self.logstory_timestamps
        
        from constants import LOGSTORY_TIMESTAMPS_FILE_PATH
        try:
            # If prefix is set, we need to make sure we don't duplicate it if get_file_content handles it
            # get_file_content uses os.path.join(self.prefix, blob_name) if self.prefix else blob_name
            # So we just pass the path relative to prefix or absolute in bucket?
            # LOGSTORY_TIMESTAMPS_FILE_PATH is "LogStory/logstory_timestamps.json"
            # If prefix is None, it fetches "LogStory/logstory_timestamps.json" which is correct.
            content = self.get_file_content(LOGSTORY_TIMESTAMPS_FILE_PATH)
            parsed = json.loads(content)
            self.logstory_timestamps = parsed if isinstance(parsed, dict) else {}
        except Exception as e:
            if self.logger:
                self.logger.warn(f"Could not load logstory timestamps from {LOGSTORY_TIMESTAMPS_FILE_PATH}: {e}")
            self.logstory_timestamps = {}
        return self.logstory_timestamps

    def get_file_content(self, blob_name, optional=False):
        """Reads a file from GCS and returns its content as a string."""
        full_path = os.path.join(self.prefix, blob_name) if self.prefix else blob_name
        blob = self.bucket.blob(full_path)
        try:
            content = blob.download_as_text()
            return content
        except Exception as e:
            if not optional:
                if self.logger:
                    self.logger.error(f"Failed to read {full_path} from bucket {self.bucket_name}: {e}")
            else:
                if self.logger:
                    self.logger.info(f"Optional file {full_path} not found in bucket {self.bucket_name}.")
            raise

    def get_soar_file_content(self, blob_name, optional=False):
        """Reads a JSON file from GCS and returns the parsed object."""
        try:
            content = self.get_file_content(blob_name, optional=optional)
            return json.loads(content)
        except Exception as e:
            if optional:
                return None
            if self.logger:
                self.logger.error(f"Failed to parse JSON content from {blob_name}: {e}")
            raise

    def list_files(self, prefix=None):
        """Lists files in the bucket with the given prefix."""
        search_prefix = os.path.join(self.prefix, prefix) if self.prefix else prefix
        blobs = self.client.list_blobs(self.bucket_name, prefix=search_prefix)
        return [blob.name for blob in blobs]

    def get_file_content_as_use_case_data(
        self, path, entry_type, file_name, name_space=None, usecase_mapping=None, usecase_name=None
    ):
        """
        Reads content from GCS and parses it using the Parser module.
        Simulates the behavior of GitManager's method.
        """
        try:
            file_content = self.get_file_content(path)
        except Exception as e:
            if self.logger:
                self.logger.error(f"Couldnt get content of file {path} from GCS")
            raise

        try:
            if entry_type == "events":
                return self.parser.parse_events(
                    file_content,
                    file_name,
                    name_space,
                    self.new_base_time,
                    self._get_logstory_timestamps() if name_space == "LogStory" else {},
                    usecase_mapping=usecase_mapping,
                    usecase_name=usecase_name,
                    logger=self.logger,
                )
            elif entry_type == "entities":
                return self.parser.parse_entities(
                    file_content,
                    file_name,
                    name_space,
                    self.new_base_time,
                    self._get_logstory_timestamps() if name_space == "LogStory" else {},
                )
            elif entry_type == "rules":
                return self.parser.parse_rules_v2(file_content, file_name)
            # Add other types if needed
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to parse file {file_name}")
                self.logger.exception(e)
            raise

    def get_generic_time_mapping(self, _folder, usecase):
        from constants import TIME_MAP_PATH
        try:
            blob_name = f"{_folder}/{TIME_MAP_PATH}"
            data = self.get_file_content(blob_name)
            if data and json.loads(data).get(usecase):
                return json.loads(data).get(usecase)
            if self.logger:
                self.logger.warn(f"No time mapping for use case {_folder}-{usecase}")
            return None
        except Exception as e:
            if self.logger:
                self.logger.warn(f"Couldnt load {_folder}-{usecase} timestamps mapping: {e}")
            return None

    def update_case(
        self, content=None, time_format=None, start_time_key=None, end_time_key=None, base_time=None
    ):
        import copy
        try:
            if time_format in ("%s", "%ms"):
                if time_format == "%ms":
                    max_endtime = max([
                        datetime.fromtimestamp(
                            float(event.get("_rawDataFields").get(end_time_key)) / 1000
                        )
                        for event in content.get("cases")[0].get("events")
                    ])
                else:
                    max_endtime = max([
                        datetime.fromtimestamp(float(event.get("_rawDataFields").get(end_time_key)))
                        for event in content.get("cases")[0].get("events")
                    ])
            else:
                max_endtime = max([
                    datetime.strptime(event.get("_rawDataFields").get(end_time_key), time_format)
                    for event in content.get("cases")[0].get("events")
                ])
            time_delta = base_time - max_endtime
            updated_content = copy.deepcopy(content)
            index = 0
            for event in content.get("cases")[0].get("events"):
                if time_format in ("%s", "%ms"):
                    if time_format == "%ms":
                        start_time = (
                            datetime.fromtimestamp(
                                float(
                                    content.get("cases")[0]
                                    .get("events")[index]
                                    .get("_rawDataFields")
                                    .get(start_time_key)
                                )
                                / 1000
                            )
                            - time_delta
                        )
                        end_time = (
                            datetime.fromtimestamp(
                                float(
                                    content.get("cases")[0]
                                    .get("events")[index]
                                    .get("_rawDataFields")
                                    .get(end_time_key)
                                )
                                / 1000
                            )
                            - time_delta
                        )
                    else:
                        start_time = (
                            datetime.fromtimestamp(
                                float(
                                    content.get("cases")[0]
                                    .get("events")[index]
                                    .get("_rawDataFields")
                                    .get(start_time_key)
                                )
                            )
                            - time_delta
                        )
                        end_time = (
                            datetime.fromtimestamp(
                                float(
                                    content.get("cases")[0]
                                    .get("events")[index]
                                    .get("_rawDataFields")
                                    .get(end_time_key)
                                )
                            )
                            - time_delta
                        )
                    updated_content["cases"][0]["events"][index]["_rawDataFields"][
                        start_time_key
                    ] = start_time.timestamp()
                    updated_content["cases"][0]["events"][index]["_rawDataFields"][end_time_key] = (
                        end_time.timestamp()
                    )
                else:
                    start_time = (
                        datetime.strptime(
                            content.get("cases")[0]
                            .get("events")[index]
                            .get("_rawDataFields")
                            .get(start_time_key),
                            time_format,
                        )
                        - time_delta
                    )
                    end_time = (
                        datetime.strptime(
                            content.get("cases")[0]
                            .get("events")[index]
                            .get("_rawDataFields")
                            .get(end_time_key),
                            time_format,
                        )
                        - time_delta
                    )
                    updated_content["cases"][0]["events"][index]["_rawDataFields"][
                        start_time_key
                    ] = start_time.strftime(time_format)
                    updated_content["cases"][0]["events"][index]["_rawDataFields"][end_time_key] = (
                        end_time.strftime(time_format)
                    )
                index += 1
            return updated_content

        except Exception as e:
            if self.logger:
                self.logger.error("Failed Updating simulated case, skipping")
                self.logger.exception(e)
            return None

    def upload_file_content(self, blob_name, content):
        """Uploads content to a blob in GCS."""
        full_path = os.path.join(self.prefix, blob_name) if self.prefix else blob_name
        blob = self.bucket.blob(full_path)
        try:
            blob.upload_from_string(content)
            if self.logger:
                self.logger.info(f"Successfully uploaded {full_path} to bucket {self.bucket_name}")
            return True
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to upload {full_path} to bucket {self.bucket_name}: {e}")
            raise

    def get_blob_first_and_last_line(self, blob_name):
        """Discovers the first and last line of a blob in O(1) time without downloading the whole file."""
        full_path = os.path.join(self.prefix, blob_name) if self.prefix else blob_name
        blob = self.bucket.get_blob(full_path)
        if not blob:
            blob = self.bucket.blob(full_path)
            blob.reload()
        
        first_line = None
        last_line = None
        try:
            if blob.size > 0:
                first_chunk = blob.download_as_bytes(start=0, end=min(blob.size, 4096) - 1).decode('utf-8', errors='ignore')
                lines = first_chunk.splitlines()
                if lines:
                    first_line = lines[0]

                start_pos = max(0, blob.size - 8192)
                last_chunk = blob.download_as_bytes(start=start_pos, end=blob.size - 1).decode('utf-8', errors='ignore')
                last_lines = last_chunk.splitlines()
                if last_lines:
                    for line in reversed(last_lines):
                        if line.strip():
                            last_line = line
                            break
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to fetch first/last line for {full_path}: {e}")
        return first_line, last_line

    def get_stream_blob_reader(self, blob_name, start_byte=0, chunk_size=1024*1024):
        """Generates (line, next_byte_offset) tuples starting from start_byte using chunked range downloads."""
        full_path = os.path.join(self.prefix, blob_name) if self.prefix else blob_name
        blob = self.bucket.get_blob(full_path)
        if not blob:
            blob = self.bucket.blob(full_path)
            blob.reload()
        
        total_size = blob.size
        current_offset = start_byte
        buffer = b""
        
        while current_offset < total_size:
            end_offset = min(current_offset + chunk_size, total_size)
            chunk = blob.download_as_bytes(start=current_offset, end=end_offset - 1)
            buffer += chunk
            
            while b"\n" in buffer:
                line_bytes, buffer = buffer.split(b"\n", 1)
                line_len = len(line_bytes) + 1
                start_byte += line_len
                yield line_bytes.decode('utf-8', errors='ignore').strip("\r"), start_byte
            
            current_offset = end_offset

        if buffer:
            line_len = len(buffer)
            start_byte += line_len
            yield buffer.decode('utf-8', errors='ignore').strip("\r"), start_byte

    def get_stream_use_case_data(
        self, path, entry_type, file_name, start_byte=0, name_space=None, usecase_mapping=None, usecase_name=None
    ):
        """
        Initializes stream data models using Parser.parse_stream_events without downloading full file contents.
        """
        try:
            if entry_type == "events":
                return self.parser.parse_stream_events(
                    self,
                    path,
                    file_name,
                    name_space,
                    self.new_base_time,
                    self._get_logstory_timestamps() if name_space == "LogStory" else {},
                    usecase_mapping=usecase_mapping,
                    usecase_name=usecase_name,
                    start_byte=start_byte,
                    logger=self.logger,
                )
            elif entry_type == "rules":
                file_content = self.get_file_content(path)
                return self.parser.parse_rules(file_content, file_name)
        except Exception as e:
            if self.logger:
                self.logger.error(f"Failed to initialize stream parser for file {file_name}")
                self.logger.exception(e)
            raise
