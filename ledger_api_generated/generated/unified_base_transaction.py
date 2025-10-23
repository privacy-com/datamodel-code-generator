# lithic-schemagen

from __future__ import annotations

from uuid import UUID

from ledger_api_generated.common import BaseResponse
from pendulum import DateTime
from pydantic import Field

from . import family, transaction_status


class UnifiedBaseTransaction(BaseResponse):
    family: family.Family = Field(..., description='The family of the transaction')
    status: transaction_status.TransactionStatus = Field(
        ..., description='The status of the transaction'
    )
    token: UUID = Field(..., description='Unique identifier for the transaction')
    created: DateTime = Field(
        ..., description='ISO 8601 timestamp of when the transaction was created'
    )
    updated: DateTime = Field(
        ..., description='ISO 8601 timestamp of when the transaction was last updated'
    )
    amount: int = Field(..., description='The amount of the transaction in cents')
    currency: str = Field(..., description='The currency of the transaction')
