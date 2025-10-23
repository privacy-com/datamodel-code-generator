# lithic-schemagen

from __future__ import annotations

from typing import Any, List

from ledger_api_generated.common import BaseResponse
from pydantic import Field


class UnifiedTransactionsResponse(BaseResponse):
    has_more: bool = Field(
        ..., description='Whether there are more transactions available'
    )
    data: List[Any] = Field(..., description='Array of unified transactions')
