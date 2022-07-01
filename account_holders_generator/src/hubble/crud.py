from datetime import datetime
from typing import TYPE_CHECKING
from uuid import uuid4

from sqlalchemy import delete

from .db import Activity

if TYPE_CHECKING:
    from sqlalchemy.orm import Session


def populate_activity_table(
    db_session: "Session", retailer_slug: str, active_campaigns: list[str], rows: int = 10
) -> None:
    if retailer_slug:
        _clear_existing_activity(db_session, retailer_slug)
    now = datetime.now().replace(microsecond=0)
    data = {
        "mid": "MC12345",
        "amount": "5.00",
        "earned": [{"type": "STAMPS", "value": "100"}],
        "datetime": str(now),
        "store_name": "Test Store",
        "transaction_id": str(uuid4()),
        "amount_currency": "GBP",
    }
    activity_rows = []
    for i in range(rows):
        activity_rows.append(
            Activity(
                type="TX_HISTORY",
                datetime=now,
                underlying_datetime=now,
                summary="test-retailer Transaction Processed for Test Store (MID: MC12345)",
                reasons=["transaction amount £5.00 meets the required threshold £1.00"],
                activity_identifier=str(uuid4()),
                user_id=str(uuid4()),
                associated_value="£5.00",
                retailer=retailer_slug,
                campaigns=active_campaigns,
                data=data,
            )
        )
    db_session.bulk_save_objects(activity_rows)
    db_session.commit()


def _clear_existing_activity(db_session: "Session", retailer_slug: str) -> None:
    db_session.execute(
        delete(Activity)
        .where(
            Activity.retailer == retailer_slug,
        )
        .execution_options(synchronize_session=False)
    )
