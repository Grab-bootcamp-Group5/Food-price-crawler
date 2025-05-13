from functools import lru_cache
from motor.motor_asyncio import AsyncIOMotorClient
import time
import random
import asyncio
from bson import ObjectId
from typing import List
import re

# metadata_db.category_shards
meta_client = AsyncIOMotorClient("mongodb://103.172.79.235:27017")
meta_db = meta_client.metadata_db_v3
category_shard_meta = meta_db.category_shards
store_branches = meta_db.store_branches


@lru_cache(maxsize=128)
def get_shard_connection(server_uri: str, db_name: str, collection_name: str):
    client = AsyncIOMotorClient(server_uri)
    db = client[db_name]
    return db[collection_name]


async def update_seasoning_category(category= "Grains & Staples"):
    
    shard = await category_shard_meta.find_one({"Category": category})
    if not shard:
        print(f"Shard not found for category: {category}")
        return

    db_name = shard["db_name"]
    collection_name = shard["collection_name"]
    server_uri = shard["server_uri"]
    collection = get_shard_connection(server_uri, db_name, collection_name)
    
    async for product in collection.find():
        name = product.get("name", "").lower()
        tmp_name = name.lower()
        match = re.search(r"(\d+[,.]?\d*)\s*(lít|l)", tmp_name)

        matches = re.findall(r"(\d+(?:\.\d+)?)\s*(g|kg|gói)\b", tmp_name)

        # ensure name  contains. gạo
        if matches and name.find("gạo") != -1 and product.get("unit") == "ml":
            value, unit = matches[-1]  # use the LAST match
            value = float(value.replace(",", "."))*1000
            unit = "g"
            await collection.update_one(
                {"_id": product["_id"]},
                {"$set": {"netUnitValue": value, "unit": unit}}
            )
            print(f"Updated product: {product['name']} to value: {value} and unit: {unit}")
            # pi
        # else:
        #     # print(f"name: {name} : value: {1} and unit: {product['unit']}")
        #     print(f"Found value: {1} and unit: {product['unit']} in product: {product['name']}")

            # Uncomment the following lines to perform the update


async def main():
    
    # Update seasoning category
    await update_seasoning_category()

if __name__ == "__main__":
    asyncio.run(main())