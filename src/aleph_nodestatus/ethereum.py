import json
import logging
import os
from functools import lru_cache
from pathlib import Path

import aiohttp
import web3
from eth_account import Account
from hexbytes import HexBytes
from requests import ReadTimeout
from web3.gas_strategies.rpc import rpc_gas_price_strategy
from aleph.sdk.chains.ethereum import ETHAccount

from .settings import settings

LOGGER = logging.getLogger(__name__)

DECIMALS = 10**18

NONCE = None


def get_aleph_account():
    account = ETHAccount(settings.ethereum_pkey)
    return account


def get_eth_account():
    if settings.ethereum_pkey:
        pri_key = HexBytes(settings.ethereum_pkey)
        try:
            account = Account.privateKeyToAccount(pri_key)
        except AttributeError:
            account = Account.from_key(pri_key)
        return account
    else:
        return None


@lru_cache(maxsize=2)
def get_web3():
    w3 = None
    if settings.ethereum_api_server:
        w3 = web3.Web3(
            web3.providers.rpc.HTTPProvider(
                settings.ethereum_api_server, request_kwargs={"timeout": 120}
            )
        )
    else:
        from web3.auto.infura import w3 as iw3

        assert w3.isConnected()
        w3 = iw3

    return w3


@lru_cache(maxsize=2)
def get_token_contract_abi():
    return json.load(
        open(os.path.join(Path(__file__).resolve().parent, "abi/ALEPHERC20.json"))
    )


@lru_cache(maxsize=2)
def get_token_contract(web3):

    address_validator = getattr(
        web3, "toChecksumAddress", getattr(web3, "to_checksum_address", None)
    )

    tokens = web3.eth.contract(
        address=address_validator(settings.ethereum_token_contract),
        abi=get_token_contract_abi(),
    )
    return tokens


def get_gas_info(web3):
    latest_block = web3.eth.get_block("latest")
    base_fee_per_gas = (
        latest_block.baseFeePerGas
    )  # Base fee in the latest block (in wei)
    max_priority_fee_per_gas = web3.to_wei(
        1, "gwei"
    )  # Priority fee to include the transaction in the block
    max_fee_per_gas = (
        5 * base_fee_per_gas
    ) + max_priority_fee_per_gas  # Maximum amount you’re willing to pay
    return max_fee_per_gas, max_priority_fee_per_gas


async def get_gas_price():
    w3 = get_web3()
    return w3.eth.generate_gas_price()


@lru_cache(maxsize=2)
def get_account():
    if settings.ethereum_pkey:
        pri_key = HexBytes(settings.ethereum_pkey)
        try:
            account = Account.privateKeyToAccount(pri_key)
        except AttributeError:
            account = Account.from_key(pri_key)
        return account
    else:
        return None


async def transfer_tokens(targets, metadata=None):
    global NONCE
    w3 = get_web3()
    address_validator = getattr(
        w3, "toChecksumAddress", getattr(w3, "to_checksum_address", None)
    )
    contract = get_token_contract(w3)
    account = get_eth_account()

    addr_count = len(targets.keys())
    total = sum(targets.values())

    LOGGER.info(f"Preparing transfer of {total} to {addr_count}")
    max_fee, max_priority = get_gas_info(w3)

    if NONCE is None:
        NONCE = w3.eth.get_transaction_count(account.address)
    tx_hash = None
    # Capture the nonce this batch is signed with so the published distribution
    # records it; the next run's pre-flight guard reads it back to detect any
    # transactions sent from this account since the last distribution.
    tx_nonce = NONCE

    success = False

    try:
        balance = contract.functions.balanceOf(account.address).call() / DECIMALS
        if total > balance:
            raise ValueError(f"Balance not enough ({total}/{balance})")

        tx = contract.functions.batchTransfer(
            [address_validator(addr) for addr in targets.keys()],
            [int(amount * DECIMALS) for amount in targets.values()],
        )
        # gas = tx.estimateGas({
        #     'chainId': 1,
        #     'gasPrice': gas_price,
        #     'nonce': NONCE,
        #     })
        # print(gas)
        tx = tx.build_transaction(
            {
                "chainId": settings.ethereum_chain_id,
                "gas": 30000 + (20000 * len(targets)),
                "nonce": NONCE,
                "maxFeePerGas": max_fee,
                "maxPriorityFeePerGas": max_priority,
            }
        )
        signed_tx = account.sign_transaction(tx)
        tx_hash = w3.eth.send_raw_transaction(signed_tx.raw_transaction).hex()
        success = True
        NONCE += 1
        LOGGER.info(f"TX {tx_hash} created on ETH")

    except Exception:
        LOGGER.exception("Error packing ethereum TX")

    if "targets" not in metadata:
        metadata["targets"] = list()
    metadata["targets"].append(
        {
            "success": success,
            "status": success and "pending" or "failed",
            "tx": tx_hash,
            "nonce": tx_nonce,
            "chain": "ETH",
            "sender": account.address,
            "targets": targets,
            "total": total,
            "contract_total": str(int(total * DECIMALS)),
        }
    )

    return metadata


def get_last_block_number(web3):
    try:
        return web3.eth.blockNumber
    except AttributeError:
        return web3.eth.block_number


# ---------------------------------------------------------------------------
# Fork-mode audited batchTransfer
#
# Production code uses `transfer_tokens` above, which broadcasts and
# returns without waiting for mining. That's intentional for --act
# runs: blocking the calculator on receipt confirmation would serialise
# batches behind block production. The fork-mode variant below is the
# opposite shape — explicit w3/contract/account, synchronous
# receipt wait, full receipt + decoded built_tx returned for the
# audit pass to consume.
#
# Kept here (next to `transfer_tokens`) so any future change to the
# batchTransfer call site is hard to make in only one of the two
# paths.
# ---------------------------------------------------------------------------

# ERC20 Transfer event ABI fragment — inlined so the audit doesn't pay
# the cost of loading the full token ABI when it only needs to decode
# one event type, and so a future Transfer-event signature change
# fails loudly here.
_ERC20_TRANSFER_EVENT_ABI = {
    "name": "Transfer",
    "type": "event",
    "anonymous": False,
    "inputs": [
        {"name": "from",  "type": "address", "indexed": True},
        {"name": "to",    "type": "address", "indexed": True},
        {"name": "value", "type": "uint256", "indexed": False},
    ],
}


def transfer_tokens_audited(
    w3, contract, account, targets,
    *, nonce: int, receipt_timeout: int = 300,
) -> dict:
    """Fork-mode `batchTransfer`. Builds, signs, broadcasts, and waits
    for the receipt; returns a dict in the same shape as
    `execute_process` (tx_hash, status, receipt, built_tx) so the
    audit helper can consume it.

    `targets` is a dict of {checksummed_address: amount_in_ALEPH_units}.
    Amounts are scaled to wei (× 10**18) here, matching the production
    `transfer_tokens` contract.
    """
    address_validator = getattr(
        w3, "toChecksumAddress", getattr(w3, "to_checksum_address", None)
    )
    max_fee, max_priority = get_gas_info(w3)
    recipients = [address_validator(a) for a in targets.keys()]
    amounts_wei = [int(v * DECIMALS) for v in targets.values()]

    tx = contract.functions.batchTransfer(recipients, amounts_wei)
    built = tx.build_transaction({
        "chainId": settings.ethereum_chain_id,
        # Same heuristic as production transfer_tokens — covers a wide
        # band of batch sizes without estimate_gas round-trips, which
        # don't add value on a deterministic fork run.
        "gas":     30_000 + (20_000 * len(targets)),
        "nonce":   nonce,
        "maxFeePerGas":         max_fee,
        "maxPriorityFeePerGas": max_priority,
    })
    signed = account.sign_transaction(built)
    tx_hash = w3.eth.send_raw_transaction(signed.raw_transaction).hex()
    LOGGER.info(f"batchTransfer tx broadcast: {tx_hash} "
                f"(recipients={len(targets)}, nonce={nonce})")
    receipt = w3.eth.wait_for_transaction_receipt(
        tx_hash, timeout=receipt_timeout,
    )
    return {
        "tx_hash":  tx_hash,
        "status":   int(receipt["status"]),
        "receipt":  receipt,
        "built_tx": {
            "from":    account.address,
            "to":      built["to"],
            "gas":     built["gas"],
            "nonce":   nonce,
            "chainId": settings.ethereum_chain_id,
            "fn":      "batchTransfer",
            "args":    {
                "recipients":  recipients,
                "amounts_wei": amounts_wei,
            },
        },
    }


def audit_distribution_tx(
    w3, *,
    tx_info: dict,
    batch_targets: dict,
    batch_index: int,
    token_address: str,
    sender_address: str,
    reconcile_bps: int = 0,
) -> dict:
    """Reconcile a broadcast `batchTransfer` against the off-chain
    reward map.

    Walks Transfer events in the receipt filtered by `token_address`
    AND `from == sender_address`, sums per recipient, and compares each
    against `batch_targets[recipient] × 10**18`. Prints a structured
    audit block and returns a dict with: pass/fail, per-recipient
    diffs, unexpected recipients (Transfers we didn't predict).

    A bps tolerance of 0 (the default for distribute) requires
    exact-wei match; the helper still accepts a positive `reconcile_bps`
    for operators who want to allow some drift.
    """
    import click

    receipt = tx_info["receipt"]
    transfer_topic0 = w3.keccak(text="Transfer(address,address,uint256)")
    token_lc  = token_address.lower()
    sender_lc = sender_address.lower()

    # Walk logs once, build a {recipient_lc: realized_wei} map.
    realized: dict = {}
    for log in receipt["logs"]:
        if (log["address"] or "").lower() != token_lc:
            continue
        if not log["topics"]:
            continue
        if bytes(log["topics"][0]) != bytes(transfer_topic0):
            continue
        from_addr = "0x" + bytes(log["topics"][1])[-20:].hex()
        if from_addr.lower() != sender_lc:
            continue
        to_addr = "0x" + bytes(log["topics"][2])[-20:].hex()
        value = int.from_bytes(bytes(log["data"]), "big")
        realized[to_addr.lower()] = realized.get(to_addr.lower(), 0) + value

    # Compute per-recipient deltas.
    mismatches = []
    total_expected = 0
    total_realized = 0
    expected_wei_per_recipient = {
        r.lower(): int(v * DECIMALS) for r, v in batch_targets.items()
    }
    for recipient_lc, expected_wei in expected_wei_per_recipient.items():
        actual_wei = realized.get(recipient_lc, 0)
        total_expected += expected_wei
        total_realized += actual_wei
        delta_wei = actual_wei - expected_wei
        if expected_wei > 0:
            delta_bps = abs(delta_wei) * 10_000 // expected_wei
        else:
            delta_bps = 0 if actual_wei == 0 else 10_000
        # With reconcile_bps == 0 (the distribute default) we want
        # *exact* wei matching, not "within 0 bps", which would otherwise
        # silently accept sub-bps wei drift on large amounts (1 wei out
        # of 2e18 == 0 bps). Off-chain rewards are scaled to exact wei
        # via int(v * DECIMALS); the Transfer event must echo that
        # exactly. With reconcile_bps > 0 the tolerance is in bps as
        # usual.
        is_violation = (
            (reconcile_bps == 0 and delta_wei != 0)
            or (reconcile_bps > 0 and delta_bps > reconcile_bps)
        )
        if is_violation:
            mismatches.append({
                "recipient": recipient_lc,
                "expected_wei": expected_wei,
                "realized_wei": actual_wei,
                "delta_wei":    delta_wei,
                "delta_bps":    delta_bps,
            })

    # Unexpected Transfers (recipients NOT in our batch). One reason
    # this matters: if the ALEPH token had a fee-on-transfer hook or
    # someone fronted a Transfer in the same tx, we want it surfaced.
    expected_lc = set(expected_wei_per_recipient.keys())
    unexpected = [
        {"recipient": r, "realized_wei": w}
        for r, w in realized.items()
        if r not in expected_lc
    ]

    failed = bool(mismatches) or bool(unexpected)
    status = "PASS" if not failed else "FAIL"

    bt = tx_info.get("built_tx", {})
    bt_args = bt.get("args", {})
    n_recipients = len(batch_targets)

    click.echo(
        f"\n=== Distribute tx audit: Batch {batch_index} "
        f"({n_recipients} recipients) === {status}"
    )
    click.echo(f"  built_tx:")
    click.echo(f"    from    : {bt.get('from')}")
    click.echo(f"    to      : {bt.get('to')}")
    click.echo(f"    fn      : batchTransfer(")
    click.echo(f"                recipients = [{n_recipients} addresses],")
    click.echo(f"                amounts    = [{n_recipients} uint256, "
               f"sum={sum(bt_args.get('amounts_wei') or []):_} wei])")
    click.echo(f"    gas     : {bt.get('gas')}")
    click.echo(f"    nonce   : {bt.get('nonce')}")
    click.echo(f"    chainId : {bt.get('chainId')}")
    # Show first three and last as a sanity sample so the user can spot
    # an obvious wrong address without dumping all N.
    sample_pairs = list(zip(
        bt_args.get("recipients") or [],
        bt_args.get("amounts_wei") or [],
    ))
    if sample_pairs:
        click.echo(f"  sample (first 3 + last):")
        for r, a in sample_pairs[:3]:
            click.echo(f"    {r}  →  {a:_} wei  "
                       f"({a / DECIMALS:.6f} ALEPH)")
        if len(sample_pairs) > 4:
            click.echo(f"    … {len(sample_pairs) - 4} more …")
        if len(sample_pairs) > 3:
            r, a = sample_pairs[-1]
            click.echo(f"    {r}  →  {a:_} wei  "
                       f"({a / DECIMALS:.6f} ALEPH)")
    click.echo(f"  receipt:")
    click.echo(f"    tx_hash : {tx_info['tx_hash']} (FORK)")
    click.echo(f"    status  : {tx_info['status']}")
    click.echo(f"    block   : {int(receipt['blockNumber']):,}")
    click.echo(f"    gas_used: {int(receipt['gasUsed']):,}")
    click.echo(f"  Transfer events:")
    click.echo(f"    matching ALEPH transfers from sender: {len(realized)}")
    click.echo(f"    total expected (wei): {total_expected:_}")
    click.echo(f"    total realized (wei): {total_realized:_}")
    click.echo(f"    delta         (wei): {total_realized - total_expected:_}")
    click.echo(f"  reconciliation:")
    click.echo(f"    per-recipient matched: "
               f"{n_recipients - len(mismatches)}/{n_recipients} "
               f"(limit ±{reconcile_bps} bps)")
    if mismatches:
        click.echo(f"    mismatches:")
        for m in mismatches[:5]:
            click.echo(
                f"      {m['recipient']}  "
                f"expected={m['expected_wei']:_}  "
                f"realized={m['realized_wei']:_}  "
                f"Δ={m['delta_wei']:_} wei ({m['delta_bps']} bps)"
            )
        if len(mismatches) > 5:
            click.echo(f"      … {len(mismatches) - 5} more …")
    if unexpected:
        click.echo(f"    unexpected transfers (not in batch):")
        for u in unexpected[:5]:
            click.echo(
                f"      {u['recipient']}  "
                f"realized={u['realized_wei']:_}"
            )
        if len(unexpected) > 5:
            click.echo(f"      … {len(unexpected) - 5} more …")

    return {
        "tx_hash":          tx_info["tx_hash"],
        "batch_index":      batch_index,
        "n_recipients":     n_recipients,
        "total_expected":   str(total_expected),
        "total_realized":   str(total_realized),
        "mismatches":       mismatches,
        "unexpected":       unexpected,
        "reconcile_failed": failed,
    }


async def get_logs_query(web3, contract, start_height, end_height, topics):
    try:
        w3_get_logs = web3.eth.getLogs
    except AttributeError:
        w3_get_logs = web3.eth.get_logs

    logs = w3_get_logs(
        {
            "address": contract.address,
            "fromBlock": start_height,
            "toBlock": end_height,
            "topics": topics,
        }
    )
    for log in logs:
        yield log


async def get_logs(web3, contract, start_height, topics=None):
    """
    try:
        logs = get_logs_query(web3, contract, start_height + 1, "latest", topics=topics)
        async for log in logs:
            yield log
    except (ValueError, ReadTimeout) as e:
        # we got an error, let's try the pagination aware version.
        if (getattr(e, 'args')
                and len(e.args)
                and not (-33000 < e.args[0]['code'] <= -32000)):
            return

        try:
            last_block = web3.eth.blockNumber
        except AttributeError:
            last_block = web3.eth.block_number
        #         if (start_height < config.ethereum.start_height.value):
        #             start_height = config.ethereum.start_height.value

        end_height = start_height + settings.ethereum_block_width_big
    """
    last_block = get_last_block_number(web3)
    end_height = start_height + settings.ethereum_block_width_big

    while True:
        try:
            logs = get_logs_query(
                web3, contract, start_height, end_height, topics=topics
            )
            async for log in logs:
                yield log

            start_height = end_height + 1
            end_height = start_height + settings.ethereum_block_width_big

            if start_height > last_block:
                LOGGER.info("Ending big batch sync")
                break

        except ValueError as e:
            if -33000 < e.args[0]["code"] <= -32000:
                last_block = get_last_block_number(web3)
                if start_height > last_block:
                    LOGGER.info("Ending big batch sync")
                    break

                end_height = start_height + settings.ethereum_block_width_small

                if end_height > last_block:
                    end_height = last_block
            else:
                raise


async def lookup_timestamp(web3, block_number, block_timestamps=None):
    if block_timestamps is not None and block_number in block_timestamps:
        return block_timestamps[block_number]
    block = web3.eth.get_block(block_number)
    if block_timestamps is not None:
        block_timestamps[block_number] = block.timestamp
    return block.timestamp
