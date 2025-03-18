import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.security import APIKeyHeader
from db_actions import Actions
from db_config import DBManager
from contextlib import asynccontextmanager
from typing import Optional
from api_config import APIConfig
from slowapi.util import get_remote_address

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_api_config() -> APIConfig:
    return APIConfig()


@asynccontextmanager
async def lifespan(app: FastAPI):

    conn = DBManager(
        os.getenv("COUCHDB_URL"),
        os.getenv("COUCHDB_USER"),
        os.getenv("COUCHDB_PASSWORD"),
    )
    db_name = os.getenv("COUCHDB_DB_NAME")
    db = conn.database(db_name)
    app.state.db = db

    conn.users_database()
    conn.create_database(db_name)
    try:
        yield
    finally:
        logger.info("Shutting down Clio")


app = FastAPI(lifespan=lifespan)

api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)


async def verify_api_key(
    request: Request,
    x_api_key: Optional[str] = Depends(api_key_header),
    config: APIConfig = Depends(get_api_config),
):
    if x_api_key != config.clio_api_key:
        client_ip = get_remote_address(request)
        logger.warning(f"Invalid API key attempt from {client_ip}")
        raise HTTPException(status_code=401, detail="Invalid API key")
    return x_api_key


@app.get("/")
def read_root():
    return {"message": "Welcome to the Clio API"}


@app.get("/ticker/{ticker}", dependencies=[Depends(verify_api_key)])
def get_data(ticker: str, config: APIConfig = Depends(get_api_config)):
    try:
        output = Actions.get_agg_data(
            db=app.state.db, api_key=config.polygon_api_key, ticker=ticker
        )
        return {"message": "Data fetched successfully", "data": output}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/ingest/{ticker}", dependencies=[Depends(verify_api_key)])
def ingest_data(ticker: str, config: APIConfig = Depends(get_api_config)):

    try:
        Actions.ingest_news_data(
            db=app.state.db, api_key=config.polygon_api_key, ticker=ticker
        )

        return {"message": "Data ingestion completed successfully"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
