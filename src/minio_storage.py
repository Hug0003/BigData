import io
import json
import uuid
import logging
from datetime import datetime
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

class MinioStorage:
    """Handles connection and operations with MinIO for raw data storage."""
    
    def __init__(self, endpoint: str, access_key: str, secret_key: str, bucket_name: str, secure: bool = False):
        self.bucket_name = bucket_name
        self.client = Minio(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            secure=secure
        )
        self._ensure_bucket_exists()

    def _ensure_bucket_exists(self):
        """Create the bucket if it doesn't already exist."""
        try:
            found = self.client.bucket_exists(self.bucket_name)
            if not found:
                self.client.make_bucket(self.bucket_name)
                logger.info(f"Bucket '{self.bucket_name}' created.")
            else:
                logger.info(f"Bucket '{self.bucket_name}' already exists.")
        except S3Error as e:
            logger.error(f"Error checking/creating bucket: {e}")
            raise

    def save_raw_json(self, data_type: str, data: dict) -> str:
        """
        Saves raw JSON data to MinIO.
        :param data_type: Identifier for the type of data (e.g., 'aeroapi_flight', 'gmaps_geocode')
        :param data: The raw data as a dictionary.
        :return: The object name (path) in MinIO.
        """
        # Generate a unique path for the object based on timestamp and data type
        timestamp = datetime.now().strftime("%Y/%m/%d/%H%M%S")
        unique_id = str(uuid.uuid4())[:8]
        object_name = f"{data_type}/{timestamp}_{unique_id}.json"

        # Convert dictionary to bytes
        json_bytes = json.dumps(data, indent=2).encode('utf-8')
        raw_data_stream = io.BytesIO(json_bytes)
        length = len(json_bytes)

        try:
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=object_name,
                data=raw_data_stream,
                length=length,
                content_type="application/json"
            )
            logger.info(f"Successfully saved {data_type} data to MinIO at {object_name}")
            return object_name
        except S3Error as e:
            logger.error(f"Error saving data to MinIO: {e}")
            raise

    def get_json(self, object_name: str) -> dict:
        """Retrieves a JSON object from MinIO and returns it as a Python dictionary."""
        try:
            response = self.client.get_object(self.bucket_name, object_name)
            data = json.loads(response.read().decode('utf-8'))
            return data
        except Exception as e:
            logger.error(f"Error reading {object_name} from MinIO: {e}")
            raise
        finally:
            if 'response' in locals():
                response.close()
            response.release_conn()
