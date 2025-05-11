from motor.motor_asyncio import AsyncIOMotorClient
import os
import asyncio
import requests
from dotenv import load_dotenv
load_dotenv()


client = AsyncIOMotorClient(os.environ.get("MONGODB_URI"))
db = client.metadata_db_v3
store_branches = db.store_branches


## with location is geospatial type 
async def update_stores_geocode():
    ## find with chain = 'cooponline'
    async for store in store_branches.find({"chain": "cooponline"}):
        # get lat, lng then update to location point
        lat = store["lat"]
        lng = store["lng"]
        location = {
            "type": "Point",
            "coordinates": [lng, lat]

        }
        print(f"store: {store['name']}, lat: {lat}, lng: {lng} => location: {location}")
        # # update location
        await store_branches.update_one(
            {"_id": store["_id"]},
            {"$set": {"location": location}}
        )

if __name__ == "__main__":
    asyncio.run(update_stores_geocode())