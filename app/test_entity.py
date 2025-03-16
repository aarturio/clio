import pytest
from pydantic import ValidationError
from app.entity import NewsEntity


@pytest.fixture
def valid_data():
    return {
        "_id": "12345",
        "root_ticker": "AAPL",
        "id": "news_001",
        "publisher": {"name": "TechCrunch", "url": "https://techcrunch.com"},
        "title": "Apple releases new product",
        "author": "John Doe",
        "published_utc": "2023-10-01T12:00:00Z",
        "article_url": "https://techcrunch.com/apple-new-product",
        "tickers": ["AAPL", "GOOGL"],
        "image_url": "https://techcrunch.com/image.jpg",
        "description": "Apple has released a new product today.",
        "keywords": ["Apple", "Product", "Release"],
        "insights": [{"type": "analysis", "content": "This is a major release."}],
    }


def test_news_entity_creation(valid_data):
    news_entity = NewsEntity(**valid_data)
    assert news_entity.hash == "12345"
    assert news_entity.root_ticker == "AAPL"
    assert news_entity.id == "news_001"
    assert news_entity.publisher["name"] == "TechCrunch"
    assert news_entity.title == "Apple releases new product"
    assert news_entity.author == "John Doe"
    assert news_entity.published_utc == "2023-10-01T12:00:00Z"
    assert news_entity.article_url == "https://techcrunch.com/apple-new-product"
    assert news_entity.tickers == ["AAPL", "GOOGL"]
    assert news_entity.image_url == "https://techcrunch.com/image.jpg"
    assert news_entity.description == "Apple has released a new product today."
    assert news_entity.keywords == ["Apple", "Product", "Release"]
    assert news_entity.insights == [
        {"type": "analysis", "content": "This is a major release."}
    ]


def test_news_entity_missing_required_fields(valid_data):
    invalid_data = valid_data.copy()
    del invalid_data["_id"]
    with pytest.raises(ValidationError):
        NewsEntity(**invalid_data)


def test_news_entity_invalid_field_type(valid_data):
    invalid_data = valid_data.copy()
    invalid_data["tickers"] = "AAPL, GOOGL"
    with pytest.raises(ValidationError):
        NewsEntity(**invalid_data)
