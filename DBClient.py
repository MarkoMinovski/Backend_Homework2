from pymongo import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://marko_m:HhfpCcGObwf7Huxn@maincluster.zwq2b.mongodb.net/?retryWrites=true&w=majority&appName=MainCluster"

client = MongoClient(uri, server_api=ServerApi('1'))

database = client["database-2"]