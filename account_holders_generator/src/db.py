import uuid

from typing import TYPE_CHECKING

from sqlalchemy import Column, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.schema import MetaData

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


metadata = MetaData()
Base = automap_base(metadata=metadata)


class AccountHolder(Base):  # type: ignore
    __tablename__ = "account_holder"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)


class Retailer(Base):  # type: ignore
    __tablename__ = "retailer"


class AccountHolderProfile(Base):  # type: ignore
    __tablename__ = "account_holder_profile"


def load_models(db_uri: str) -> "Session":
    engine = create_engine(db_uri, poolclass=NullPool)
    SessionMaker = sessionmaker(bind=engine)
    Base.prepare(engine, reflect=True)
    return SessionMaker()
