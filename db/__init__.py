from datetime import datetime
from db.models import Session, Price, StoreBranch  # cập nhật tùy file của bạn

async def upsert_price(store: str, sku: str, price: float):
    async with Session() as session:
        async with session.begin():
            rec = await session.get(Price, (store, sku))
            if rec:
                rec.price = price
                rec.ts = datetime.utcnow()
            else:
                new_price = Price(store=store, sku=sku, price=price, ts=datetime.utcnow())
                session.add(new_price)

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
