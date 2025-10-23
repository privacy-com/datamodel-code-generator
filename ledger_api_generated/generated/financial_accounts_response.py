# lithic-schemagen

from __future__ import annotations

from typing import List

from ledger_api_generated.common import BaseResponse

from . import financial_account_response


class FinancialAccountsResponse(BaseResponse):
    data: List[financial_account_response.FinancialAccountResponse]
    has_more: bool
