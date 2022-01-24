from typing import TYPE_CHECKING

from sqlalchemy import create_engine
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.schema import MetaData

if TYPE_CHECKING:
    from sqlalchemy.ext.automap import AutomapBase
    from sqlalchemy.orm.session import Session


metadata = MetaData()
Base: "AutomapBase" = automap_base(metadata=metadata)


class RetailerRewards(Base):
    __tablename__ = "retailer_rewards"


class Campaign(Base):
    __tablename__ = "campaign"


class RewardRule(Base):
    __tablename__ = "reward_rule"


class EarnRule(Base):
    __tablename__ = "earn_rule"


def load_models(db_uri: str) -> "Session":
    engine = create_engine(db_uri, poolclass=NullPool)
    Base.prepare(engine, reflect=True)
    return scoped_session(sessionmaker(bind=engine))
