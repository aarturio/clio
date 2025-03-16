from pydantic import BaseModel, Field
from dotenv import load_dotenv

load_dotenv()

from typing import List, Dict


class DataEntity(BaseModel):
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


class PriceEntity(BaseModel):
    hash: str = Field(..., alias="_id")
    root_ticker: str
    id: str
    date: str
    open: float
    close: float
    high: float
    low: float
    volume: int

    class Config:
        populate_by_name = True
