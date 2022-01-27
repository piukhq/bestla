import uuid

from typing import TYPE_CHECKING

from sqlalchemy import Column, ForeignKey, Integer, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.schema import MetaData

if TYPE_CHECKING:
    from sqlalchemy.ext.automap import AutomapBase
    from sqlalchemy.orm.session import Session

metadata = MetaData()
Base: "AutomapBase" = automap_base(metadata=metadata)


class Reward(Base):
    __tablename__ = "reward"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4, index=True)
    reward_config_id = Column(Integer, ForeignKey("reward_config.id", ondelete="CASCADE"), nullable=False)


class RewardConfig(Base):
    __tablename__ = "reward_config"


def load_models(db_uri: str) -> "Session":
    engine = create_engine(db_uri, poolclass=NullPool)

    Base.prepare(engine, reflect=True)
    return scoped_session(sessionmaker(bind=engine))
