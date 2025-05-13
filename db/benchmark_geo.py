import asyncio
import asyncpg
from motor.motor_asyncio import AsyncIOMotorClient
import time

# --- Config ---

MONGO_URI = "mongodb://103.172.79.235:27017"
POSTGRES_DSN = "postgresql://postgres:asd123@103.172.79.28:5432/store_branches"
CENTER_LON, CENTER_LAT = 106.7, 10.8
RADIUS_LIST_KM = [40]

# --- Benchmark MongoDB ---
async def benchmark_mongodb():
    client = AsyncIOMotorClient(MONGO_URI)
    collection = client.metadata_db_v3.store_branches

    print("\n[MongoDB Benchmark]")
    for radius_km in RADIUS_LIST_KM:
        radius_m = radius_km * 1000

        query = {
            "location": {
                "$nearSphere": {
                    "$geometry": {
                        "type": "Point",
                        "coordinates": [CENTER_LON, CENTER_LAT]
                    },
                    "$maxDistance": radius_m
                }
            }
        }

        start = time.perf_counter()
        docs = await collection.find(query).to_list(length=10)
        duration = time.perf_counter() - start
        print(f"Radius {radius_km} km: {len(docs)} docs in {duration:.4f} sec")

# --- Benchmark PostgreSQL ---
async def benchmark_postgresql():
    conn = await asyncpg.connect(dsn=POSTGRES_DSN)

    print("\n[PostgreSQL Benchmark]")
    for radius_km in RADIUS_LIST_KM:
        radius_m = radius_km * 1000

        query = f"""
            SELECT * FROM stores
            WHERE ST_DWithin(
                location,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                $3
            )
            ORDER BY ST_Distance(
                location,
                ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
            )
        """

        start = time.perf_counter()
        rows = await conn.fetch(query, CENTER_LON, CENTER_LAT, radius_m)
        duration = time.perf_counter() - start
        print(f"Radius {radius_km} km: {len(rows)} rows in {duration:.4f} sec")

    await conn.close()

# --- Main ---
async def main():
    await benchmark_mongodb()
    await benchmark_postgresql()

asyncio.run(main())
