import uuid

from typing import TYPE_CHECKING

from sqlalchemy import BIGINT, Column, ForeignKey, create_engine
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


class AccountHolder(Base):
    __tablename__ = "account_holder"

    account_holder_uuid = Column(UUID(as_uuid=True), default=uuid.uuid4)
    opt_out_token = Column(UUID(as_uuid=True), default=uuid.uuid4)


class RetailerConfig(Base):
    __tablename__ = "retailer_config"


class AccountHolderProfile(Base):
    __tablename__ = "account_holder_profile"

    account_holder_id = Column(BIGINT, ForeignKey("account_holder.id", ondelete="CASCADE"))


class AccountHolderMarketingPreference(Base):
    __tablename__ = "account_holder_marketing_preference"

    account_holder_id = Column(BIGINT, ForeignKey("account_holder.id", ondelete="CASCADE"))


class AccountHolderReward(Base):
    __tablename__ = "account_holder_reward"

    reward_uuid = Column(UUID(as_uuid=True), default=uuid.uuid4)
    account_holder_id = Column(BIGINT, ForeignKey("account_holder.id", ondelete="CASCADE"))


class AccountHolderPendingReward(Base):
    __tablename__ = "account_holder_pending_reward"

    account_holder_id = Column(BIGINT, ForeignKey("account_holder.id", ondelete="CASCADE"))


class AccountHolderTransactionHistory(Base):
    __tablename__ = "account_holder_transaction_history"

    account_holder_id = Column(BIGINT, ForeignKey("account_holder.id", ondelete="CASCADE"))


class AccountHolderCampaignBalance(Base):
    __tablename__ = "account_holder_campaign_balance"

    account_holder_id = Column(BIGINT, ForeignKey("account_holder.id", ondelete="CASCADE"))


def load_models(db_uri: str) -> "Session":
    engine = create_engine(db_uri, poolclass=NullPool, echo=settings.SQL_DEBUG)

    Base.prepare(engine, reflect=True)
    return scoped_session(sessionmaker(bind=engine))
