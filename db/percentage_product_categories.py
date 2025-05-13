from functools import lru_cache
from motor.motor_asyncio import AsyncIOMotorClient
import time
import random
import asyncio
from bson import ObjectId
from typing import List


centralized_client = AsyncIOMotorClient("mongodb://sysAdmin:Conchimnon_03@160.191.49.128:27017/centralized_products")
centralized_db = centralized_client["centralized_products"]
centralized_store_branches = centralized_db.store_branches
centralized_products = centralized_db.products


# --- Benchmark centralized find_one ---
async def percentage_category_distribution():

    ## Percentage of each category is total of that category / totoal count
    pipeline = [
        {"$group": {"_id": "$category", "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$project": {
            "_id": 0,
            "category": "$_id",
            "count": 1
        }}
    ]
    cursor = centralized_products.aggregate(pipeline)
    result = [doc async for doc in cursor]
    total_count = sum(item['count'] for item in result)
    for item in result:
        item['percentage'] = (item['count'] / total_count) * 100
    for data in result:
        print(f"Category: {data['category']}")
        print(f"Percentage: {data['percentage']}" )
        # write to csv
        with open('category_distribution.csv', 'a') as f:
            f.write(f"{data['category']},{data['percentage']}\n")
    # print(f"Total count: {total_count}")
    return result

async def main():
    result = await percentage_category_distribution()
    print("\n[Category Distribution]")          
    # for data in result:
    #     print(f"Category: {data['_id']}, Count: {data['count']}")
    #     print(f"Percentage: {data['count'] / total_count * 100:.2f}%")

if __name__ == "__main__":
    asyncio.run(main())