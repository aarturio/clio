import os
import json

from pydantic import BaseModel, Field
from dotenv import load_dotenv
from .zzz import generate_id, convert_timestamp

load_dotenv()

import requests
import json
from typing import List, Dict, Any


class CouchDBConnector:
    def __init__(self, url: str, username: str = None, password: str = None):
        """Initialize with CouchDB server URL and optional credentials."""
        self.url = url.rstrip("/")
        self.auth = (username, password) if username and password else None
        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth

    def database(self, db_name: str) -> "CouchDBDatabase":
        """Return a database object for the given database name."""
        return CouchDBDatabase(self, db_name)

    def create_database(self, db_name: str) -> bool:
        """Create a database if it doesn’t exist."""
        response = self.session.put(f"{self.url}/{db_name}")
        return response.status_code in (201, 202)

    def delete_database(self, db_name: str) -> bool:
        """Delete a database."""
        response = self.session.delete(f"{self.url}/{db_name}")
        return response.status_code == 200


class CouchDBDatabase:
    def __init__(self, connector: CouchDBConnector, db_name: str):
        """Initialize with a connector and database name."""
        self.connector = connector
        self.db_url = f"{connector.url}/{db_name}"

    def bulk_docs(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Bulk insert/update documents."""
        payload = {"docs": docs}
        response = self.connector.session.post(
            f"{self.db_url}/_bulk_docs", json=payload
        )
        response.raise_for_status()
        return response.json()


class NewsEntity(BaseModel):
    hash: str = Field(..., alias="_id")
    root_ticker: str
    id: str
    publisher: Dict[str, str]
    title: str
    author: str
    published_utc: str
    article_url: str
    tickers: List[str]
    image_url: str
    description: str
    keywords: List[str]
    insights: List[Dict[str, str]]

    class Config:
        populate_by_name = True


class DBOps(BaseModel):

    @staticmethod
    def ingest_news(db: CouchDBDatabase, ticker: str):

        API_KEY = os.getenv("POLYGON_API_KEY")
        url = f"https://api.polygon.io/v2/reference/news?ticker={ticker}&order=desc&limit=1000&sort=published_utc&apiKey={API_KEY}"
        r = requests.get(url)
        data = r.json()

        batch = []

        for item in data["results"]:
            doc = NewsEntity(
                hash=generate_id(item["title"], item["article_url"]),
                root_ticker=ticker,
                id=item["id"],
                publisher=item["publisher"],
                title=item["title"],
                author=item["author"],
                published_utc=item["published_utc"],
                article_url=item["article_url"],
                tickers=item["tickers"],
                image_url=item["image_url"],
                description=item["description"],
                keywords=item["keywords"],
                insights=item["insights"],
            )
            batch.append(doc.model_dump(by_alias=True))

        db.bulk_docs(batch)

    @staticmethod
    def get_news(db: CouchDBDatabase, ticker: str):
        output = [db.get(x) for x in db if db.get(x)["ticker"] == ticker]

        return output
