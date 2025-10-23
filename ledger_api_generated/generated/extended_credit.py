# lithic-schemagen

from __future__ import annotations

from ledger_api_generated.common import BaseResponse


class ExtendedCredit(BaseResponse):
    credit_extended: int
