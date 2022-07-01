from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import Column, create_engine
from sqlalchemy.dialects.postgresql import UUID
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.pool import NullPool
from sqlalchemy.sql.schema import MetaData

from config import settings

if TYPE_CHECKING:
    from sqlalchemy.ext.automap import AutomapBase
    from sqlalchemy.orm.session import Session

metadata = MetaData()
Base: "AutomapBase" = automap_base(metadata=metadata)


class Activity(Base):
    __tablename__ = "activity"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid4, index=True)


def load_models(db_uri: str) -> "Session":
    engine = create_engine(db_uri, poolclass=NullPool, echo=settings.SQL_DEBUG)

    Base.prepare(engine, reflect=True)
    return scoped_session(sessionmaker(bind=engine))
