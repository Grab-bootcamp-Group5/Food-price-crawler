from datetime import datetime
from db.models import Session, ProductPrice, StoreBranch  # cập nhật tùy file của bạn


def parse_date_safe(date_str):
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        return None

async def upsert_product(product: dict):
    async with Session() as session:
        async with session.begin():
            rec = await session.get(ProductPrice, (product["store"], product["sku"]))
            if rec:
                rec.name = product["name"]
                rec.unit = product["unit"]
                rec.price = product["price"]
                rec.discount = product["discount"]
                rec.promotion = product["promotion"]
                rec.image = product["image"]
                rec.link = product["link"]
                rec.excerpt = product["excerpt"]
                rec.date_begin = parse_date_safe(product["date_begin"])
                rec.date_end = parse_date_safe(product["date_end"])
                rec.ts = datetime.utcnow()
            else:
                new_product = ProductPrice(
                    store=product["store"],
                    sku=product["sku"],
                    name=product["name"],
                    unit=product["unit"],
                    price=product["price"],
                    discount=product["discount"],
                    promotion=product["promotion"],
                    image=product["image"],
                    link=product["link"],
                    excerpt=product["excerpt"],
                    date_begin=parse_date_safe(product["date_begin"]),
                    date_end=parse_date_safe(product["date_end"]),
                    ts=datetime.utcnow()
                )
                session.add(new_product)


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
