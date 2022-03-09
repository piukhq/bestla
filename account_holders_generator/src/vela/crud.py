from typing import TYPE_CHECKING

from sqlalchemy import delete
from sqlalchemy.future import select

from ..fixtures import campaign_payload, earn_rule_payload, reward_rule_payload
from .db import Campaign, EarnRule, RetailerRewards, RewardRule

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from account_holders_generator.src.carina.db import RewardConfig
    from account_holders_generator.src.polaris.db import RetailerConfig


def get_active_campaigns_and_reward_rules(
    db_session: "Session", retailer_config: "RetailerConfig", reward_config: "RewardConfig", campaign_default: str
) -> tuple[list[str], dict]:
    campaigns = db_session.scalars(
        select(Campaign).where(
            Campaign.status == "ACTIVE",
            Campaign.retailer_id == RetailerRewards.id,
            RetailerRewards.slug == retailer_config.slug,
        )
    ).all()
    reward_rules = {}
    for campaign in campaigns:
        reward_rules[campaign.slug] = db_session.scalars(
            select(RewardRule).where(
                RewardRule.reward_slug == reward_config.reward_slug, RewardRule.campaign_id == campaign.id
            )
        ).all()

    if not campaigns:
        return [campaign_default], reward_rules
    else:
        return [campaign.slug for campaign in campaigns], reward_rules


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
