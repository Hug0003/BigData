import os
import time
import logging
from dotenv import load_dotenv

from api_clients import OpenSkyClient, GeoapifyClient
from minio_storage import MinioStorage

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
logger = logging.getLogger(__name__)

# Number of flights to enrich with reverse geocoding per run
MAX_GEOCODE = 10
# Delay between Geoapify calls to respect rate limits (seconds)
GEOCODE_DELAY = 0.3


class DataPipeline:
    """Orchestrates raw data collection from APIs and storage in MinIO (data lake)."""

    def __init__(self):
        load_dotenv()

        self.opensky_client = OpenSkyClient(
            username=os.getenv("OPENSKY_USERNAME"),
            password=os.getenv("OPENSKY_PASSWORD"),
        )
        self.geo_client = GeoapifyClient(api_key=os.getenv("GEOAPIFY_API_KEY"))

        self.storage = MinioStorage(
            endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
            bucket_name=os.getenv("MINIO_BUCKET_NAME", "raw-api-data"),
            secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        )

    def ingest_opensky(self) -> list:
        """Fetch all aircraft states from OpenSky and save raw JSON to MinIO.

        Returns the list of state vectors for downstream enrichment.
        """
        logger.info("Fetching aircraft states from OpenSky Network...")
        states_data = self.opensky_client.get_all_states()

        object_name = self.storage.save_raw_json("opensky_states", states_data)
        states = states_data.get("states", [])
        logger.info(f"Saved {len(states)} aircraft states to MinIO at {object_name}")
        return states

    def enrich_with_geocoding(self, states: list, max_geocode: int = MAX_GEOCODE):
        """Reverse-geocode a sample of flights and save each result to MinIO.

        Only processes flights that have valid longitude/latitude values.
        """
        # Filter flights with valid coordinates (index 5=lon, 6=lat)
        valid = [s for s in states if s[5] is not None and s[6] is not None]
        sample = valid[:max_geocode]

        logger.info(f"Enriching {len(sample)} flights with Geoapify reverse geocoding...")
        for state in sample:
            callsign = (state[1] or "unknown").strip()
            lon, lat = state[5], state[6]
            try:
                geocode_data = self.geo_client.reverse_geocode(lat, lon)
                # Attach the callsign so we can link back to the flight later
                geocode_data["_callsign"] = callsign
                self.storage.save_raw_json("geoapify_geocode", geocode_data)
                logger.info(f"  [{callsign}] geocoded ({lat:.4f}, {lon:.4f})")
            except Exception as e:
                logger.warning(f"  [{callsign}] geocoding failed: {e}")
            time.sleep(GEOCODE_DELAY)

    def run(self, max_geocode: int = MAX_GEOCODE):
        """Run the full ingestion pipeline: OpenSky → MinIO, Geoapify → MinIO."""
        logger.info("=== Starting raw data ingestion pipeline ===")
        try:
            states = self.ingest_opensky()
            if states:
                self.enrich_with_geocoding(states, max_geocode=max_geocode)
            else:
                logger.warning("No aircraft states returned by OpenSky.")
            logger.info("=== Ingestion pipeline completed successfully ===")
        except Exception as e:
            logger.error(f"Pipeline failed: {e}", exc_info=True)
            raise


if __name__ == "__main__":
    pipeline = DataPipeline()
    pipeline.run()
