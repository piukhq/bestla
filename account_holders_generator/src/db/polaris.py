import uuid

from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.schema import MetaData

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


metadata = MetaData()
Base = automap_base(metadata=metadata)


class AccountHolder(Base):  # type: ignore
    __tablename__ = "account_holder"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)


class RetailerConfig(Base):  # type: ignore
    __tablename__ = "retailer_config"


class AccountHolderProfile(Base):  # type: ignore
    __tablename__ = "account_holder_profile"

    account_holder_id = Column(UUID(as_uuid=True), ForeignKey("account_holder.id", ondelete="CASCADE"))


class UserVoucher(Base):  # type: ignore
    __tablename__ = "user_voucher"

    account_holder_id = Column(UUID(as_uuid=True), ForeignKey("account_holder.id", ondelete="CASCADE"))


def load_models(db_uri: str) -> "Session":
    engine = create_engine(db_uri, poolclass=NullPool)

    Base.prepare(engine, reflect=True)
    return scoped_session(sessionmaker(bind=engine))
