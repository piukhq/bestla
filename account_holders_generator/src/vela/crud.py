from typing import TYPE_CHECKING

from sqlalchemy import delete
from sqlalchemy.future import select

from ..fixtures import campaign_payload, earn_rule_payload, reward_rule_payload
from .db import Campaign, EarnRule, RetailerRewards, RewardRule

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from account_holders_generator.src.polaris.db import RetailerConfig


def get_active_campaigns(db_session: "Session", retailer: "RetailerConfig", campaign_default: str) -> list[str]:
    campaigns = db_session.scalars(
        select(Campaign.slug).where(
            Campaign.status == "ACTIVE",
            Campaign.retailer_id == RetailerRewards.id,
            RetailerRewards.slug == retailer.slug,
        )
    )

    if not campaigns:
        return [campaign_default]
    else:
        return [campaign for campaign in campaigns]


def setup_retailer_reward_and_campaign(
    db_session: "Session", retailer_slug: str, campaign_slug: str, reward_slug: str, refund_window: int
) -> None:
    db_session.execute(
        delete(RetailerRewards)
        .where(RetailerRewards.slug == retailer_slug)
        .execution_options(synchronize_session=False)
    )
    retailer = RetailerRewards(slug=retailer_slug)
    db_session.add(retailer)
    db_session.flush()
    campaign = Campaign(**campaign_payload(retailer.id, campaign_slug))
    db_session.add(campaign)
    db_session.flush()
    db_session.add(RewardRule(**reward_rule_payload(campaign.id, reward_slug, refund_window)))
    db_session.add(EarnRule(**earn_rule_payload(campaign.id)))
    db_session.commit()
