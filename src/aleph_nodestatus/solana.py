import asyncio
import copy
import json
import logging
from functools import lru_cache

import aiohttp
from aleph_client.asynchronous import create_post

from solana.rpc.async_api import AsyncClient
from solana.rpc.core import TransactionExpiredBlockheightExceededError
from solana.rpc.types import TxOpts
from solana.transaction import Transaction
from spl.token.constants import TOKEN_PROGRAM_ID
from spl.token.instructions import transfer, TransferParams, get_associated_token_address
from solders.pubkey import Pubkey
from solders.keypair import Keypair

from .ethereum import get_logs, get_web3
from .settings import settings

LOGGER = logging.getLogger(__name__)

DECIMALS = 10**settings.solana_decimals


@lru_cache(maxsize=2)
def get_client():
    return AsyncClient(settings.solana_endpoint)


@lru_cache(maxsize=2)
def get_account():
    if settings.solana_pkey_file:
        with open(settings.solana_pkey_file, 'r') as file:
            return Keypair.from_json(str(json.load(file)))
    if settings.solana_pkey:
        return Keypair.from_json(str(settings.solana_pkey))
    else:
        return None


async def transfer_tokens(targets, metadata=None):
    addr_count = len(targets.keys())
    total = sum(targets.values())

    LOGGER.info(f"Preparing transfer of {total} wALEPH to {addr_count} addresses on Solana")

    mint = Pubkey.from_string(settings.solana_mint)

    sender = get_account()
    if sender is None:
        LOGGER.error("No sender account configured for Solana")
        return metadata

    sender_ata = get_associated_token_address(sender.pubkey(), mint)

    client = get_client()

    balance = (await client.get_account_info(sender_ata)).value.lamports / DECIMALS
    if total >= balance:
        raise ValueError(f"Balance not enough ({total}/{balance})")

    def split_in_batches(d, n=22):
        it = iter(d)
        batch = {}
        for key in it:
            batch[key] = d[key]
            if len(batch) == n:
                yield batch
                batch = {}
        if batch:
            yield batch

    async def send_transaction(tx: Transaction):
        tx_copy = copy.deepcopy(tx)
        try:
            blockhash = (await client.get_latest_blockhash()).value.blockhash
            tx.recent_blockhash = blockhash
            tx.sign(sender)
            serialized_tx = tx.serialize()
            signature = (await client.send_raw_transaction(serialized_tx, TxOpts(skip_confirmation=False))).value
            LOGGER.info(f"TX {signature} created on SOL")
            return signature
        except TransactionExpiredBlockheightExceededError as e:
            LOGGER.error(f"Transaction failed: {e}")
            return tx_copy

    transactions = []
    for batch in split_in_batches(targets):
        transaction = Transaction()

        for address, balance in batch.items():
            receiver_public_key = Pubkey.from_string(address)
            receiver_ata = get_associated_token_address(receiver_public_key, mint)
            ata_info = await client.get_account_info(receiver_ata)
            if ata_info.value is None:
                LOGGER.warning(f"No associated token account for {address}")
                continue

            rounded_balance = round(balance, 2)
            amount_to_transfer = int(rounded_balance * DECIMALS)

            instruction = transfer(TransferParams(
                amount=amount_to_transfer,
                source=sender_ata,
                owner=sender.pubkey(),
                dest=receiver_ata,
                program_id=TOKEN_PROGRAM_ID,
            ))
            transaction.add(instruction)

        if not transaction.instructions:
            continue

        transactions.append(send_transaction(transaction))

    results = await asyncio.gather(*transactions)
    failed_txs = [tx for tx in results if isinstance(tx, Transaction)]
    signatures = [tx for tx in results if isinstance(tx, str)]
    retries = 0
    while failed_txs and retries < 3:
        await asyncio.sleep(5)
        LOGGER.info(f"Retrying {len(failed_txs)} failed transactions")
        results = await asyncio.gather(*[send_transaction(tx) for tx in failed_txs])
        failed_txs = [tx for tx in results if isinstance(tx, Transaction)]
        signatures += [tx for tx in results if isinstance(tx, str)]

    if failed_txs:
        LOGGER.error(f"Failed to send {len(failed_txs)} transactions")
        for tx in failed_txs:
            LOGGER.error(tx)
    await client.close()

    success = len(failed_txs) == 0

    if "targets" not in metadata:
        metadata["targets"] = list()
    metadata["targets"].append(
        {
            "success": success,
            "status": success and "pending" or "failed",
            "tx": signatures,
            "chain": "SOL",
            "sender": str(sender.pubkey()),
            "targets": targets,
            "total": total,
            "contract_total": str(int(total * DECIMALS)),
        }
    )

    return metadata


async def update_balances(account, main_height, balances):
    return await create_post(
        account,
        {
            "tags": ["SOL", "SPL", settings.solana_mint, settings.filter_tag],
            "main_height": main_height,
            "platform": "{}_{}".format(settings.token_symbol, "SOL"),
            "token_contract": settings.solana_mint,
            "token_symbol": settings.token_symbol,
            "chain": "SOL",
            "balances": {
                addr: value / DECIMALS for addr, value in balances.items() if value > 0
            },
        },
        settings.balances_post_type,
        channel=settings.aleph_channel,
        api_server=settings.aleph_api_server,
    )


async def query_balances(endpoint, mint):
    query = (
        """
query {
  tokenHolders(
    mint: "%s"
  ) {
    account
    owner
    balance
  }
}"""
        % mint
    )
    async with aiohttp.ClientSession() as session:
        async with session.post(endpoint, json={"query": query}) as resp:
            result = await resp.json()
            holders = result["data"]["tokenHolders"]
            seen_accounts = set()
            values = {}
            for h in holders:
                if h is None:
                    continue
                if h["account"] not in seen_accounts:
                    seen_accounts.add(h["account"])
                    values[h["owner"]] = values.get(h["owner"], 0) + int(h["balance"])
            return values
            # return {h['owner']: int(h['balance']) for h in holders}


async def solana_monitoring_process():
    from .messages import get_aleph_account

    web3 = get_web3()
    account = get_aleph_account()

    previous_balances = None

    while True:
        changed_items = set()

        balances = await query_balances(
            settings.solana_endpoint, settings.solana_mint
        )
        if previous_balances is None:
            changed_items = set(balances.keys())
        else:
            for address in previous_balances.keys():
                if address not in balances.keys():
                    changed_items.add(address)

            for address, amount in balances.items():
                if (
                    amount != previous_balances.get(address, 0)
                    and address not in settings.solana_ignored_addresses
                ):
                    changed_items.add(address)

        if changed_items:
            print(changed_items)
            await update_balances(account, web3.eth.block_number, balances)
            previous_balances = balances

        await asyncio.sleep(300)
