import logging
import os

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from db import DBOps, CouchDBConnector
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):

    conn = CouchDBConnector(
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
        # conn.delete_database(db_name)
        pass


app = FastAPI(lifespan=lifespan)


@app.get("/")
def read_root():
    return {"message": "Welcome to the Clio API"}


@app.get("/ticker/{ticker}")
def get_data(ticker: str):
    try:
        output = DBOps.get_news(db=app.state.db, ticker=ticker)
        return {"message": "News fetched successfully", "data": output}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/ingest/")
def ingest_data():

    try:
        DBOps.ingest_news(db=app.state.db)

        return {"message": "Data ingestion completed successfully"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
