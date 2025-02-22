import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from .db import DBOps, CouchDBConnector
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):

    # Connect to CouchDB
    conn = CouchDBConnector(
        os.getenv("URL"), os.getenv("USERNAME"), os.getenv("PASSWORD")
    )
    db = conn.database("news_db")
    app.state.db = db

    # Create the database (optional)
    conn.create_database("news_db")
    try:
        yield  # App runs during this block
    finally:
        conn.delete_database("news_db")


app = FastAPI(lifespan=lifespan)


@app.get("/ticker/{ticker}")
def get_news(ticker: str):
    try:
        output = DBOps.get_news(db=app.state.db, ticker=ticker)
        return output
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/ingest/")
def ingest_data():

    try:
        DBOps.ingest_news(db=app.state.db)

        return {"message": "Data ingestion completed successfully"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
