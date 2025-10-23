# lithic-schemagen

from __future__ import annotations

from typing import Any, Union

from ledger_api_generated.common import BaseResponse
from pydantic import Field


class UnifiedTransaction(BaseResponse):
    __root__: Any = Field(
        ...,
        description='A super class for all transaction types in the unified ledger',
        title='UnifiedTransaction',
    )
