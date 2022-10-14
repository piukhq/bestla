from typing import TYPE_CHECKING

from sqlalchemy import delete
from sqlalchemy.future import select

from ..fixtures import campaign_payload, earn_rule_payload, reward_rule_payload
from .db import Campaign, EarnRule, RetailerRewards, RewardRule

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from account_holders_generator.src.polaris.db import RetailerConfig


def get_active_campaigns(
    db_session: "Session", retailer: "RetailerConfig", campaign_default: str, loyalty_type: str
) -> list[str]:
    campaigns = (
        db_session.execute(
            select(Campaign.slug).where(
                Campaign.status == "ACTIVE",
                Campaign.retailer_id == RetailerRewards.id,
                Campaign.loyalty_type == loyalty_type,
                RetailerRewards.slug == retailer.slug,
            )
        )
        .scalars()
        .all()
    )

    if not campaigns:
        return [campaign_default]
    return campaigns


def get_campaign(db_session: "Session", campaign_slug: str) -> Campaign:
    return db_session.execute(
        select(Campaign).where(
            Campaign.slug == campaign_slug,
        )
    ).scalar_one()


def get_reward_rule(db_session: "Session", campaign_slug: str) -> RewardRule:
    campaign = get_campaign(db_session, campaign_slug)
    return db_session.execute(
        select(RewardRule).where(
            RewardRule.campaign_id == campaign.id,
        )
    ).scalar_one()


def setup_retailer_reward_and_campaign(
    db_session: "Session",
    retailer_slug: str,
    campaign_slug: str,
    reward_slug: str,
    refund_window: int | None,
    loyalty_type: str,
) -> None:
    db_session.execute(
        delete(RetailerRewards)
        .where(RetailerRewards.slug == retailer_slug)
        .execution_options(synchronize_session=False)
    )
    if loyalty_type == "STAMPS":
        refund_window = None
    retailer = RetailerRewards(slug=retailer_slug)
    db_session.add(retailer)
    db_session.flush()
    campaign = Campaign(**campaign_payload(retailer.id, campaign_slug, loyalty_type))
    db_session.add(campaign)
    db_session.flush()
    db_session.add(RewardRule(**reward_rule_payload(campaign.id, reward_slug, refund_window)))
    db_session.add(EarnRule(**earn_rule_payload(campaign.id, loyalty_type)))
    db_session.commit()
