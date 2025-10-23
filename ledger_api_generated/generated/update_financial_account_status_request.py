# lithic-schemagen

from __future__ import annotations

from typing import Optional

from ledger_api_generated.common import BaseResponse

from . import financial_account_status, update_financial_account_substatus


class UpdateFinancialAccountStatusRequest(BaseResponse):
    status: financial_account_status.FinancialAccountStatus
    substatus: Optional[
        update_financial_account_substatus.UpdateFinancialAccountSubstatus
    ]
