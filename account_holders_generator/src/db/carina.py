import uuid

from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Integer, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.schema import MetaData

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


metadata = MetaData()
Base = automap_base(metadata=metadata)


class Voucher(Base):
    __tablename__ = "voucher"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    voucher_config_id = Column(Integer, ForeignKey("voucher_config.id", ondelete="CASCADE"), nullable=False)


class VoucherConfig(Base):
    __tablename__ = "voucher_config"


def load_models(db_uri: str) -> "Session":
    engine = create_engine(db_uri, poolclass=NullPool)

    Base.prepare(engine, reflect=True)
    return scoped_session(sessionmaker(bind=engine))
