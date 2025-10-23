# lithic-schemagen

from __future__ import annotations

from typing import Optional

from ledger_api_generated.common import BaseResponse
from pendulum import Date


class ExternalPaymentActionRequest(BaseResponse):
    memo: Optional[str] = None
    effective_date: Date
