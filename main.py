import logging

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from db import DBManager, DBOps
from contextlib import asynccontextmanager

logging.basicConfig(level=logging.INFO)
load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):

    db_manager = DBManager()
    app.state.db_manager = db_manager
    try:
        yield  # App runs during this block
    finally:
        db_manager.close()


app = FastAPI(lifespan=lifespan)


@app.get("/ticker/{ticker}")
def get_news(ticker: str):
    try:
        db_manager = app.state.db_manager
        output = DBOps.get_news(db_manager=db_manager, ticker=ticker)
        return output
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/ingest/")
def ingest_data():

    try:
        db_manager = app.state.db_manager
        DBOps.ingest_news(db_manager)

        return {"message": "Data ingestion completed successfully"}

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
