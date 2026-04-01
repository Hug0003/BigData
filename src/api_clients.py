import requests
import logging

logger = logging.getLogger(__name__)

class OpenSkyClient:
    """Client for interacting with OpenSky Network API."""
    BASE_URL = "https://opensky-network.org/api/states/all"

    def __init__(self, username=None, password=None):
        self.auth = None
        if username and password:
            self.auth = (username, password)

    def get_all_states(self) -> dict:
        """Fetch current states of all aircraft."""
        logger.info("Fetching all aircraft states from OpenSky Network")
        # OpenSky allows 1 request every 10 seconds for anonymous users
        response = requests.get(self.BASE_URL, auth=self.auth)
        response.raise_for_status()
        return response.json()

class GeoapifyClient:
    """Client for interacting with Geoapify Reverse Geocoding API."""
    BASE_URL = "https://api.geoapify.com/v1/geocode/reverse"

    def __init__(self, api_key: str):
        self.api_key = api_key

    def reverse_geocode(self, lat: float, lng: float) -> dict:
        """Convert GPS coordinates (latitude, longitude) into an address via Geoapify."""
        params = {
            "lat": lat,
            "lon": lng,
            "apiKey": self.api_key
        }
        logger.info(f"Reverse geocoding coordinates ({lat}, {lng}) via Geoapify API")
        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()
        return response.json()
