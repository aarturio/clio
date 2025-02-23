import hashlib


def generate_id(str1, str2):
    return hashlib.md5(f"{str1}{str2}".encode()).hexdigest()
