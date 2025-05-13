from functools import lru_cache
from motor.motor_asyncio import AsyncIOMotorClient
import time
import random
import asyncio
from bson import ObjectId
from typing import List

# metadata_db.category_shards
meta_client = AsyncIOMotorClient("mongodb://103.172.79.235:27017")
meta_db = meta_client.metadata_db_v3
category_shard_meta = meta_db.category_shards
store_branches = meta_db.store_branches

centralized_client = AsyncIOMotorClient("mongodb://sysAdmin:Conchimnon_03@160.191.49.128:27017/centralized_products")
centralized_db = centralized_client["centralized_products"]
centralized_store_branches = centralized_db.store_branches
centralized_products = centralized_db.products


async def get_random_n_name(n: int):
    pipeline = [
        {"$group": {"_id": {"name_en": "$name_en", "category": "$category"}}},
        {"$sample": {"size": n}},
        {"$project": {
            "_id": 0,
            "name_en": "$_id.name_en",
            "category": "$_id.category"
        }}
    ]
    cursor = centralized_products.aggregate(pipeline)
    return [doc async for doc in cursor]


# --- Benchmark centralized find_one ---
async def benchmark_centralized(products: list):
    print("\n[Centralized Database Benchmark]")
    start = time.perf_counter()
    for data in products:
        result = await centralized_products.find_one({"name_en": data["name_en"]})
        # Optional: print(f"Found: {result['name_en']}" if result else "Not Found")
    duration = time.perf_counter() - start
    print(f"Checked {len(products)} products in {duration:.4f} sec")

# --- Reuse shard connection by category ---
@lru_cache(maxsize=128)
def get_shard_connection(server_uri: str, db_name: str, collection_name: str):
    client = AsyncIOMotorClient(server_uri)
    db = client[db_name]
    return db[collection_name]

# --- Benchmark decentralized (sharded) find_one ---
async def benchmark_descentralized(products: list):
    print("\n[Sharded Database Benchmark]")
    start = time.perf_counter()
    for data in products:
        category = data["category"]
        shard = await category_shard_meta.find_one({"Category": category})
        if not shard:
            print(f"Shard not found for category: {category}")
            continue

        db_name = shard["db_name"]
        collection_name = shard["collection_name"]
        server_uri = shard["server_uri"]
        collection = get_shard_connection(server_uri, db_name, collection_name)

        result = await collection.find_one({"name_en": data["name_en"]})
        # Optional: print(f"Found: {result['name_en']}" if result else "Not Found")
    duration = time.perf_counter() - start
    print(f"Checked {len(products)} products in {duration:.4f} sec")

# --- Main entry ---
async def main():
    n = [10, 50, 100, 200, 500]
    for i in n:
        print(f"Testing with {i} random products...")
        products = await get_random_n_name(i)
        await benchmark_centralized(products)
        await benchmark_descentralized(products)
        print(f"Finished testing with {i} random products.\n")
        await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())