import os
import logging
from dotenv import load_dotenv

from api_clients import OpenSkyClient, GeoapifyClient
from minio_storage import MinioStorage

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

class DataPipeline:
    """Orchestrates API calls and MinIO data storage for OpenSky and Geoapify."""
    
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        # Initialize Clients
        # OpenSky is used here without authentication (anonymous limited to 1 call/10s)
        self.opensky_client = OpenSkyClient()
        self.geo_client = GeoapifyClient(api_key=os.getenv("GEOAPIFY_API_KEY", "c94d10c092e54fae803f888688104f4e"))
        
        # Initialize MinIO Storage
        self.storage = MinioStorage(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            bucket_name=os.getenv("MINIO_BUCKET_NAME", "raw-api-data"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true"
        )

    def run_pipeline(self):
        """
        Executes the pipeline:
        1. Query OpenSky for all current aircraft states.
        2. Save raw states data to MinIO.
        3. Extract the first aircraft with valid coordinates.
        4. Query Geoapify for reverse geocoding on that coordinate.
        5. Save raw geocoding data to MinIO.
        """
        logger.info("--- Starting OpenSky + Geoapify Data Pipeline ---")
        
        try:
            # 1. Get raw OpenSky states
            states_data = self.opensky_client.get_all_states()
            
            # 2. Store raw states data to MinIO
            self.storage.save_raw_json("opensky_states", states_data)
            
            # 3. Extract sample coordinate for reverse geocoding
            states = states_data.get("states", [])
            if states:
                # Iterate to find the first flight with valid lat/lon
                # OpenSky state vector: [0]=icao24, [1]=callsign, [2]=origin, ..., [5]=longitude, [6]=latitude
                target_flight = None
                for s in states:
                    if s[5] is not None and s[6] is not None:
                        target_flight = s
                        break
                
                if target_flight:
                    lon, lat = target_flight[5], target_flight[6]
                    callsign = target_flight[1].strip() if target_flight[1] else "Unknown"
                    logger.info(f"Found sample flight {callsign} at ({lat}, {lon})")
                    
                    # 4. Get raw Geoapify reverse geocoding data
                    geocode_data = self.geo_client.reverse_geocode(lat, lon)
                    
                    # 5. Store raw geocoding data to MinIO
                    self.storage.save_raw_json("geoapify_geocode", geocode_data)
                else:
                    logger.warning("No flight with valid coordinates found in the current states.")
            else:
                logger.warning("No states data returned from OpenSky.")
                
            logger.info("--- Data Pipeline execution completed successfully ---")

        except Exception as e:
            logger.error(f"Pipeline failed: {e}")


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run_pipeline()
