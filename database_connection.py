import os
from pymongo import MongoClient


def get_db_session():
    try:
        uri = os.getenv("MONGODB_URL")
        return MongoClient(host=uri)
    except Exception as e:
        raise e

