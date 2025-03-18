from dotenv import load_dotenv

load_dotenv()

import requests
from typing import List, Dict, Any


class DBManager:
    def __init__(self, url: str, username: str = None, password: str = None):
        """Initialize with CouchDB server URL and optional credentials."""
        self.url = url.rstrip("/")
        self.auth = (username, password) if username and password else None
        self.session = requests.Session()
        if self.auth:
            self.session.auth = self.auth

    def database(self, db_name: str) -> "DBOperations":
        """Return a database object for the given database name."""
        return DBOperations(self, db_name)

    def create_database(self, db_name: str) -> bool:
        """Create a database if it doesn’t exist."""
        response = self.session.put(f"{self.url}/{db_name}")
        return response.status_code in (201, 202)

    def delete_database(self, db_name: str) -> bool:
        """Delete a database."""
        response = self.session.delete(f"{self.url}/{db_name}")
        return response.status_code == 200

    def users_database(self) -> bool:
        """Ensure the _users database exists."""
        response = self.session.put(f"{self.url}/_users")
        return response.status_code in (201, 202, 412)  # 412 means it already exists


class DBOperations:
    def __init__(self, connector: DBManager, db_name: str):
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

    def get_docs(self, query) -> List[Dict[str, Any]]:
        """Get documents based on a query."""
        response = self.connector.session.post(f"{self.db_url}/_find", json=query)
        response.raise_for_status()
        return response.json()

    def add_index(self, column) -> bool:
        """Create an index."""
        query = {
            "index": {
                "fields": [column],
            },
            "name": f"{column}-index",
            "type": "json",
        }
        response = self.connector.session.post(f"{self.db_url}/_index", json=query)
        return response.status_code in (201, 202)
