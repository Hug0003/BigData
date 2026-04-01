import os
import logging
from dotenv import load_dotenv

from api_clients import OpenSkyClient, GeoapifyClient, AeroAPIClient
from minio_storage import MinioStorage

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

class DataPipeline:
    """Orchestrates API calls and MinIO data storage using environment variables."""
    
    def __init__(self):
        # Load environment variables from .env
        load_dotenv()
        
        # Initialize Clients directly from environment variables
        self.opensky_client = OpenSkyClient()
        
        # AeroAPI using the secret from .env (replaces the old JSON file logic)
        self.aero_client = AeroAPIClient(api_key=os.getenv("AEROAPI_CLIENT_SECRET"))
        
        self.geo_client = GeoapifyClient(api_key=os.getenv("GEOAPIFY_API_KEY"))
        
        # Initialize MinIO Storage
        self.storage = MinioStorage(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            bucket_name=os.getenv("MINIO_BUCKET_NAME", "raw-api-data"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
        )

    def run_opensky_pipeline(self):
        """Pipeline using OpenSky data."""
        logger.info("--- Starting OpenSky + Geoapify Data Pipeline ---")
        try:
            states_data = self.opensky_client.get_all_states()
            self.storage.save_raw_json("opensky_states", states_data)
            
            states = states_data.get("states", [])
            if states:
                # Find first flight with valid coordinates
                target_flight = next((s for s in states if s[5] and s[6]), None)
                if target_flight:
                    lon, lat = target_flight[5], target_flight[6]
                    geocode_data = self.geo_client.reverse_geocode(lat, lon)
                    self.storage.save_raw_json("geoapify_geocode", geocode_data)
                    logger.info("Pipeline execution completed successfully")
        except Exception as e:
            logger.error(f"OpenSky Pipeline failed: {e}")

if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_opensky_pipeline()
