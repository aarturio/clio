import hashlib


def generate_id(str1, str2):
    """
    Generate a unique ID by concatenating two strings and hashing them with MD5.
    """
    return hashlib.md5(f"{str1}{str2}".encode()).hexdigest()


def format_query(ticker, field, delta):
    """
    Send a query to the database and return the result.
    """
    query = {
        "selector": {
            "root_ticker": ticker,
            field: {"$gte": delta},
        },
        "limit": 1000,
        "sort": [{field: "asc"}],
    }
    return query
