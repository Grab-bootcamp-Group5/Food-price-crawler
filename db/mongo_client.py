from motor.motor_asyncio import AsyncIOMotorClient

client = AsyncIOMotorClient("mongodb://103.172.79.235:27017")
db = client.product_price_db
product_prices = db.product_prices
store_branches = db.store_branches
