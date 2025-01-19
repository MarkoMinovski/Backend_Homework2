import os

from pymongo import MongoClient
from pymongo.server_api import ServerApi

uri = os.getenv("MONGO_URI")

client = MongoClient(uri, server_api=ServerApi('1'))

database = client["database-2"]
