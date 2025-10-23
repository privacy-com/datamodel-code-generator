# lithic-schemagen

from __future__ import annotations

from typing import Optional

from ledger_api_generated.common import BaseResponse

from . import financial_account_status, internal_update_financial_account_substatus


class InternalUpdateFinancialAccountStatusRequest(BaseResponse):
    status: financial_account_status.FinancialAccountStatus
    substatus: Optional[
        internal_update_financial_account_substatus.InternalUpdateFinancialAccountSubstatus
    ]
