# lithic-schemagen

from __future__ import annotations

from typing import Optional

from ledger_api_generated.common import BaseResponse
from pydantic import Field


class TransferDetailedResult(BaseResponse):
    result: str = Field(..., description='Detailed result code')
    reason: Optional[str] = Field(None, description='Reason for the result')
