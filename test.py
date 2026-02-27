import os
from dotenv import load_dotenv
from pymongo import MongoClient

# Load environment variables
load_dotenv()

MONGO_URI = os.getenv("MONGO_URL")
DB_NAME = os.getenv("DB_NAME")  # Make sure DB_NAME is also in .env

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client[DB_NAME]
collection = db["appliedjobs"]

# Aggregation to get unique users count
pipeline = [
    {
        "$group": {
            "_id": "$userId"
        }
    },
    {
        "$count": "uniqueUsers"
    }
]

result = list(collection.aggregate(pipeline))

if result:
    print("Unique users who applied to at least 1 job:", result[0]["uniqueUsers"])
else:
    print("Unique users who applied to at least 1 job: 0")