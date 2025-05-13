import asyncio
from motor.motor_asyncio import AsyncIOMotorClient
import asyncpg

MONGO_URI = "mongodb://103.172.79.235:27017"
POSTGRES_DSN = "postgresql://postgres:asd123@103.172.79.28:5432/store_branches"

mongo_client = AsyncIOMotorClient(MONGO_URI)
store_branches = mongo_client.metadata_db_v3.store_branches  # update as needed

async def fetch_and_insert():
    # try:
    pg_conn = await asyncpg.connect(POSTGRES_DSN)
    print("Connected to PostgreSQL")

    async for branch in store_branches.find():
        print(branch)
        coords = branch.get("location", {}).get("coordinates", [None, None])
        if not coords or None in coords:
            continue

        try:
            print(f"Inserting {branch.get('store_id')}")
            await pg_conn.execute("""
                INSERT INTO stores (
                    chain, store_id, name, address,
                    province_id, province_name,
                    district_id, district_name,
                    ward_id, location
                )
                VALUES (
                    $1, $2, $3, $4,
                    $5, $6,
                    $7, $8,
                    $9, ST_SetSRID(ST_MakePoint($10, $11), 4326)::geography
                )
            """, 
            branch.get("chain"),
            str(branch.get("store_id")) if branch.get("store_id") else None,
            branch.get("name"),
            branch.get("address"),
            int(branch.get("provinceId")) if branch.get("provinceId") else None,
            branch.get("provinceName"),
            int(branch.get("districtId")) if branch.get("districtId") else None,
            branch.get("districtName"),
            int(branch.get("wardId")) if branch.get("wardId") else None,
            coords[0], coords[1]
            )

        except Exception as e:
            print(f"Failed to insert {branch.get('store_id')}: {e}")

    await pg_conn.close()

asyncio.run(fetch_and_insert())
