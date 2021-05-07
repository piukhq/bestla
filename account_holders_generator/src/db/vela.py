from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.schema import MetaData

if TYPE_CHECKING:
    from sqlalchemy.orm.session import Session


metadata = MetaData()
Base = automap_base(metadata=metadata)


class RetailerRewards(Base):  # type: ignore
    __tablename__ = "retailer_rewards"


class Campaign(Base):  # type: ignore
    __tablename__ = "campaign"


def load_models(db_uri: str) -> "Session":
    engine = create_engine(db_uri, poolclass=NullPool)
    Base.prepare(engine, reflect=True)
    return scoped_session(sessionmaker(bind=engine))
