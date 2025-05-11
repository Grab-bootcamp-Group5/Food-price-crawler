from datetime import datetime
from functools import lru_cache
from motor.motor_asyncio import AsyncIOMotorClient


def parse_date_safe(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        return None

# metadata_db.category_shards
meta_client = AsyncIOMotorClient("mongodb://103.172.79.235:27017")
meta_db = meta_client.metadata_db_v3
category_shard_meta = meta_db.category_shards
store_branches = meta_db.store_branches

@lru_cache(maxsize=128)  # cache để tránh mở kết nối lại nhiều lần
def get_shard_connection(server_uri: str, db_name: str, collection_name: str):
    client = AsyncIOMotorClient(server_uri)
    db = client[db_name]
    return db[collection_name]


async def upsert_product(product: dict, category_en: str):
    # Truy metadata để tìm db/collection/server
    meta = await category_shard_meta.find_one({"Category": category_en})
    if not meta:
        print(f"Metadata not found for category: {category_en}")
        return
    
    db_name = meta["db_name"]
    collection_name = meta["collection_name"]
    server_uri = meta["server_uri"]
    
    collection = get_shard_connection(server_uri, db_name, collection_name)
    
    filter_query = {"store": product["store_id"], "sku": product["sku"]}

    update_data = {
        "$set": {
            "sku": product["sku"],
            "name": product["name"],
            "name_en": product["name_en"],
            "unit": product["unit"].lower(), 
            "netUnitValue": product["netUnitValue"], 
            "token_ngrams": product["token_ngrams"],
            "category": category_en,
            "store_id": product["store_id"],
            "url": product["url"],
            "image": product["image"],
            "promotion": product.get("promotion", ""),
            "price": product["price"],
            "sysPrice": product["sysPrice"],
            "dicountPercent": product["discountPercent"],
            "date_begin": parse_date_safe(product["date_begin"]),
            "date_end": parse_date_safe(product["date_end"]),
            "crawled_at": datetime.utcnow().isoformat(),
        }
    }
    await collection.update_one(filter_query, update_data, upsert=True)

async def upsert_branch(branch_dict: dict):
    # Convert lon, lat to GeoJSON format if they exist
    if "lng" in branch_dict and "lat" in branch_dict:
        branch_dict["location"] = {
            "type": "Point",
            "coordinates": [branch_dict["lng"], branch_dict["lat"]]
        }
        # Remove lon and lat fields as they are now part of location
        branch_dict.pop("lng")
        branch_dict.pop("lat")
    
    filter_query = {
        "store_id": branch_dict["store_id"],
        "chain": branch_dict["chain"]
    }
    update_data = {
        "$set": branch_dict  # cập nhật toàn bộ fields
    }
    print(f"Upserting branch: {branch_dict['store_id']}, {branch_dict['chain']}")
    await store_branches.update_one(filter_query, update_data, upsert=True)

async def fetch_branches(chain: str):

    ## Get all branches that have chain = bhx
    branches = await store_branches.find({"chain": chain}).to_list(length=None)
    print(f"Found {len(branches)} branches for chain {chain}")
    return branches