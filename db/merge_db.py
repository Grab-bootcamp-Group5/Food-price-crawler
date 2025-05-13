from functools import lru_cache
from motor.motor_asyncio import AsyncIOMotorClient

# metadata_db.category_shards
meta_client = AsyncIOMotorClient("mongodb://103.172.79.235:27017")
meta_db = meta_client.metadata_db_v3
category_shard_meta = meta_db.category_shards
store_branches = meta_db.store_branches

centralized_client = AsyncIOMotorClient("mongodb://sysAdmin:Conchimnon_03@160.191.49.128:27017/centralized_products")
centralized_db = centralized_client["centralized_products"]
centralized_store_branches = centralized_db.store_branches
centralized_products = centralized_db.products


@lru_cache(maxsize=128)  # cache để tránh mở kết nối lại nhiều lần
def get_shard_connection(server_uri: str, db_name: str, collection_name: str):
    client = AsyncIOMotorClient(server_uri)
    db = client[db_name]
    return db[collection_name]

async def copy_products():
    # meta will get all distinct categories
    async for category in category_shard_meta.find():
        db_name = category["db_name"]
        collection_name = category["collection_name"]
        server_uri = category["server_uri"]

        collection = get_shard_connection(server_uri, db_name, collection_name)

        # Get all products from the shard
        async for product in collection.find():
            # Check if the product already exists in the centralized database
            existing_product = await centralized_products.find_one({"_id": product["_id"]})
            if not existing_product:
                # Insert the product into the centralized database
                await centralized_db.products.insert_one(product)
                print(f"Inserted product {product['sku']} from {db_name}.{collection_name}")
            else:
                print(f"Product {product['sku']} already exists, skipping")

async def copy_branches():
    async for branch in store_branches.find():
        existing = await centralized_store_branches.find_one({"_id": branch["_id"]})
        if not existing:
            await centralized_store_branches.insert_one(branch)
            print(f"Inserted branch {branch['_id']}")
        else:
            print(f"Branch {branch['_id']} already exists, skipping")



async def main():
    # await copy_branches()
    await copy_products()
    # print("All products copied to centralized database")

if __name__ == "__main__":
    import asyncio
    asyncio.run(main())