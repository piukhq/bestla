from typing import TYPE_CHECKING

from .db import Campaign, RetailerRewards

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from account_holders_generator.src.polaris.db import RetailerConfig


def get_active_campaigns(db_session: "Session", retailer: "RetailerConfig", campaign_default: str) -> list[str]:
    campaigns = (
        db_session.query(Campaign.slug)
        .join(RetailerRewards)
        .filter(
            RetailerRewards.slug == retailer.slug,
            Campaign.status == "ACTIVE",
        )
        .all()
    )
    if not campaigns:
        return [campaign_default]
    else:
        return [campaign[0] for campaign in campaigns]
