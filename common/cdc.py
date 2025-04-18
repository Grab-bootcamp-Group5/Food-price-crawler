import asyncio
from db.models import Session, Price

async def is_price_changed(store: str, sku: str, new_price: float, pct=0.01) -> bool:
    async with Session() as s:
        async with s.begin():
            rec = await s.get(Price, (store, sku))
            if not rec:
                return True
            diff = abs(rec.price - new_price) / rec.price
            return diff >= pct
