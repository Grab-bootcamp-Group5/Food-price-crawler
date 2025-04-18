import httpx, asyncio
from pydantic import BaseModel
from datetime import datetime

class Product(BaseModel):
    store: str
    sku: str
    title: str
    price: float
    unit: str
    crawled_at: datetime

async def fetch_page(group_id:int, page:int=1)->list[Product]:
    url = STORES["cooponline"]["base"]
    params = {**STORES["cooponline"]["params"],
              "group_id": group_id, "page": page}
    async with httpx.AsyncClient(timeout=20) as client:
        r = await client.get(url, params=params)
        r.raise_for_status()
        data = r.json()["products"]
        return [Product(store="cooponline",
                        sku=p["sku"],
                        title=p["name"],
                        price=float(p["price"]),
                        unit=p["unit"],
                        crawled_at=datetime.utcnow())
                for p in data]

## Crawl all pages of a group (1/4 day)
async def crawl_group(group_id:int):
    first = await fetch_page(group_id, 1)
    results = first
    total = math.ceil(len(first)/40)  
    for page in range(2, total+1):
        results += await fetch_page(group_id, page)
        await asyncio.sleep(1)        # politeness
    return results


