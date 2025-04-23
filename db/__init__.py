from datetime import datetime
from db.mongo_client import product_prices


def parse_date_safe(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        return None


async def upsert_product(product: dict):
    filter_query = {"store": product["store"], "sku": product["sku"]}
    update_data = {
        "$set": {
            "name": product["name"],
            "unit": product["unit"],
            "price": product["price"],
            "discount": product["discount"],
            "promotion": product["promotion"],
            "image": product["image"],
            "link": product["link"],
            "excerpt": product["excerpt"],
            "date_begin": parse_date_safe(product["date_begin"]),
            "date_end": parse_date_safe(product["date_end"]),
            "store_id": product["store_id"],
            "category": product["category"],
            "ts": datetime.utcnow(),
        }
    }
    await product_prices.update_one(filter_query, update_data, upsert=True)


async def upsert_branch(branch_dict: dict):
    async with Session() as session:
        async with session.begin():
            pk = (branch_dict["id"], branch_dict["chain"])
            rec = await session.get(StoreBranch, pk)
            if rec:
                for k, v in branch_dict.items():
                    setattr(rec, k, v)
            else:
                new_branch = StoreBranch(**branch_dict)
                session.add(new_branch)
