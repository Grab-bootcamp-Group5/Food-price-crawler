from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy import Column, String, Float, DateTime, Integer, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime

Base = declarative_base()

class Price(Base):
    __tablename__ = "product_price"
    store = Column(String, primary_key=True)
    sku = Column(String, primary_key=True)
    price = Column(Float, nullable=False)
    ts = Column(DateTime, default=datetime.utcnow)

class StoreBranch(Base):
    __tablename__ = "store_branch"
    id        = Column(String, primary_key=True)           # '415'
    chain     = Column(String, primary_key=True)           # 'cooponline'
    name      = Column(String)
    address   = Column(String)
    phone     = Column(String)
    city      = Column(String)
    district  = Column(String)
    ward      = Column(String)
    lat       = Column(Float)
    lon       = Column(Float)
    updated   = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

engine = create_async_engine("sqlite+aiosqlite:///prices.db", future=True)
Session = sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
