# lithic-schemagen

from __future__ import annotations

from typing import Optional

from ledger_api_generated.common import BaseResponse

from . import financial_account_state


class PatchFinancialAccountRequest(BaseResponse):
    financial_account_state: Optional[financial_account_state.FinancialAccountState] = (
        None
    )
    is_spend_blocked: Optional[bool] = None
