from motor.motor_asyncio import AsyncIOMotorClient
import os
import asyncio
import requests
from dotenv import load_dotenv
load_dotenv()


client = AsyncIOMotorClient(os.environ.get("MONGODB_URI"))
db = client.metadata_db
store_branches = db.store_branches


## with location is geospatial type 
async def update_stores_geocode():
    async for store in store_branches.find({"location": {"$exists": True}}):
        if "location" in store:
            location = store["location"]
            if "coordinates" in location:
                coordinates = location["coordinates"]
                ## If coordinates is a string, convert it to a list of floats
                if isinstance(coordinates, list) and all(isinstance(coord, str) for coord in coordinates):
    
                    coordinates = [float(coord) for coord in coordinates]
                    await store_branches.update_one(
                                {"_id": store["_id"]},
                                {"$set": {"location": {"type": "Point", "coordinates": coordinates}}}
                            )
                    # print(f"Updating store {store['_id']} with coordinates: {coordinates}")
                ## if coordinates is 0,0
                if coordinates == [0, 0]:
                    # print(f"Store {store['_id']} has invalid coordinates: {coordinates}")
                    GOOGLE_MAP_API_KEY = "5b3ce3597851110001cf6248d1ce92ee54f24baebad009ac57d0a452"
                    store_name = store["name"]

                    # Construct the geocoding API URL
                    api_url = f"https://api.openrouteservice.org/geocode/search?api_key={GOOGLE_MAP_API_KEY}&text={store_name}"

                    try:
                        # Make a request to the geocoding API
                        response = requests.get(api_url)
                        response.raise_for_status()
                        data = response.json()

                        # Extract the first set of coordinates from the response
                        if data.get("features"):
                            first_feature = data["features"][0]
                            lon, lat = first_feature["geometry"]["coordinates"]

                            # Update the store with the new location
                            await store_branches.update_one(
                                {"_id": store["_id"]},
                                {"$set": {"location": {"type": "Point", "coordinates": [lon, lat]}}}
                            )
                            print(f"Updated store {store['_id']} with geocode: {lat}, {lon}")
                        else:
                            print(f"No valid geocode found for store {store['_id']} with name: {store_name}")
                    except Exception as e:
                        print(f"Error fetching geocode for store {store['_id']} with name: {store_name}. Error: {e}")
                #     lat, lon = 

                #     # Update the store with the new geocode
                #     await store_branches.update_one(
                #         {"_id": store["_id"]},
                #         {"$set": {"geocode": {"lat": lat, "lon": lon}}}
                #     )
                #     print(f"Updated store {store['_id']} with geocode: {lat}, {lon}")
                # else:
                #     print(f"Invalid coordinates for store {store['_id']}: {coordinates}")
            else:
                print(f"No coordinates found for store {store['_id']}")

if __name__ == "__main__":
    asyncio.run(update_stores_geocode())