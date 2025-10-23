# lithic-schemagen

from __future__ import annotations

from typing import List
from uuid import UUID

from ledger_api_generated.common import BaseResponse
from pendulum import DateTime
from pydantic import Field

from . import transaction_result, transfer_detailed_result


class PaymentTransactionEvent(BaseResponse):
    amount: int = Field(..., description='Event amount in cents')
    type: str = Field(..., description='Event type')
    result: transaction_result.TransactionResult = Field(
        ..., description='Event result'
    )
    created: DateTime = Field(
        ..., description='ISO 8601 timestamp of when the event was created'
    )
    token: UUID = Field(..., description='Unique identifier for the event')
    detailed_results: List[transfer_detailed_result.TransferDetailedResult] = Field(
        ..., description='Detailed results for the event'
    )
