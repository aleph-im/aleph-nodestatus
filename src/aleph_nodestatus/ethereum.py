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

    success = False

    try:
        balance = contract.functions.balanceOf(account.address).call() / DECIMALS
        if total >= balance:
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
        tx_hash = w3.eth.send_raw_transaction(signed_tx.rawTransaction).hex()
        success = True
        NONCE += 1
        LOGGER.info(f"TX {tx_hash} created on ETH")

    except:
        LOGGER.exception("Error packing ethereum TX")

    if "targets" not in metadata:
        metadata["targets"] = list()
    metadata["targets"].append(
        {
            "success": success,
            "status": success and "pending" or "failed",
            "tx": tx_hash,
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
