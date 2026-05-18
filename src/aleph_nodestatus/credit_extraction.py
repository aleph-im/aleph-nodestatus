"""Orchestration for the credit-extraction CLI.

Wraps payment_processor.extract_aleph with the operational glue that
used to live inside process_credit_distribution: admin-account
resolution, ETH preflight, async offload of the sync web3 calls, and a
human-readable stdout summary.

No Aleph audit post is written. The on-chain process() transactions
are the canonical record of every extraction; ops review happens via
stdout / log aggregation.
"""

import asyncio
import logging
from typing import Optional

from .ethereum import get_web3
from .payment_processor import (
    extract_aleph,
    get_processor_contract,
    get_quoter_contract,
    get_v2_router_contract,
    get_v4_quoter_contract,
)
from .settings import settings

LOGGER = logging.getLogger(__name__)


async def process_credit_extraction(
    *, act: bool, dry_run: bool, transfer: bool,
    slippage_bps: Optional[int] = None,
) -> dict:
    """Run one extract pass. Returns the dict produced by extract_aleph."""
    web3 = get_web3()

    processor = get_processor_contract(web3)
    quoters = {
        "v2": get_v2_router_contract(web3),
        "v3": get_quoter_contract(web3),
        "v4": get_v4_quoter_contract(web3),
    }

    extract_block = await asyncio.to_thread(
        extract_aleph,
        web3, processor, quoters,
        account=None,
        from_address=settings.payment_processor_admin_address,
        dry_run=dry_run or not transfer,
        transfer_enabled=transfer,
        slippage_bps=(
            slippage_bps if slippage_bps is not None
            else settings.process_slippage_bps
        ),
    )
    return extract_block
