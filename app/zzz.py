import hashlib
from datetime import datetime


def generate_id(str1, str2):
    return hashlib.md5(f"{str1}{str2}".encode()).hexdigest()


def convert_timestamp(timestamp_str: str) -> str:
    """Convert timestamp from YYYYMMDDTHHmmss to YYYY-MM-DD HH:MM:SS"""
    try:
        dt = datetime.strptime(timestamp_str, "%Y%m%dT%H%M%S")
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError as e:
        raise ValueError(
            f"Invalid timestamp format: {timestamp_str}. Expected: YYYYMMDDTHHMMSS"
        ) from e
