import logging
import os

logger = logging.getLogger(__name__)


class APIConfig:
    def __init__(self):
        self.clio_api_key = os.getenv("CLIO_API_KEY")
        self.polygon_api_key = os.getenv("POLYGON_API_KEY")

        if not self.clio_api_key:
            logger.error("Missing CLIO_API_KEY")
            raise ValueError("CLIO_API_KEY not set in environment")
        if not self.polygon_api_key:
            logger.error("Missing POLYGON_API_KEY")
            raise ValueError("POLYGON_API_KEY not set in environment")
