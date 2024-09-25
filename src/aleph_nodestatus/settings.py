from typing import List, Dict

from pydantic import BaseSettings


class Settings(BaseSettings):
    app_name: str = "aleph.im swapper"
    admin_email: str = "hello@aleph.im"
    # account_pkey: str = Field(..., env='ACCT_PKEY')
    # account_type: str = Field(..., env='ACCT_TYPE')
    aleph_channel: str = "TEST"
    aleph_api_server: str = "https://api2.aleph.im"
    token_symbol: str = "ALEPH"
    chain_name: str = "ETH"

    ethereum_api_server: str = None
    ethereum_token_contract: str = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
    ethereum_total_supply: int = 500000000
    ethereum_deployer: str = "0xb6e45ADfa0C7D70886bBFC990790d64620F1BAE8"
    ethereum_chain_id: int = 1
    ethereum_pkey: str = ""
    ethereum_min_height: int = 10939070
    ethereum_decimals: int = 18
    ethereum_swap_fee: int = 10
    ethereum_blocks_per_day: int = 7130
    ethereum_batch_size: int = 200
    ethereum_block_width_big: int = 30000
    ethereum_block_width_small: int = 500

    ethereum_sablier_contract: str = "0xCD18eAa163733Da39c232722cBC4E8940b1D8888"
    ethereum_sablier_min_height: int = 13245838

    balances_platforms: List[str] = ["ALEPH_ETH_SABLIER", "ALEPH_SOL"]
    balances_senders: List[str] = ["0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10"]

    scores_senders: List[str] = ["0x4D52380D3191274a04846c89c069E6C3F2Ed94e4"]
    scores_post_type: str = "aleph-scoring-scores"

    reward_start_height: int = 11519440
    reward_nodes_daily: int = 15000
    reward_stakers_daily_base: int = 15000
    reward_resource_node_monthly: int = 550
    reward_resource_node_monthly_base: int = 250
    reward_resource_node_monthly_variable: int = 1250

    bonus_modifier: float = 1.25
    bonus_start: int = 12020360
    bonus_decay: float = 0.0000001

    staking_threshold: int = 9999
    node_threshold: int = 199999
    node_activation: int = 500000

    node_max_linked: int = 8
    node_max_paid: int = 5

    node_post_type: str = "corechan-operation"
    balances_post_type: str = "balances-update"
    # staker_post_type: str = "corechan-delegation"
    filter_tag: str = "mainnet"

    platform_solana_endpoint: str = "https://spltoken.api.aleph.cloud/"
    platform_solana_mint: str = "CsZ5LZkDS7h9TDKjrbL7VAwQZ9nsRu8vJLhRYfmGaN8K"
    platform_solana_decimals: int = 6
    platform_solana_ignored_addresses: List[str] = [
        "CuieVDEDtLo7FypA9SbLM9saXFdb1dsshEkyErMqkRQq",
        "DVtR63sAnJPM9wdt1hYBqA5GTyFzjfcfdLTfsSzV85Ss",
        "HKt6xFufxTBBs719WQPbro9t1DfDxffurxFhTPntMgoe",
        "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1",
        "3EBEwydfyiVvFbXFYWENcZFrnb2ihCVauqcHzcCUNfwA",
        "3HixSznhPFXA8ueB2sy9iVmVCZP7McRSR4wUuCVeHfh4",
        "7MBLg6oV5phip11YBbJPuq7u38kdzSi9PM3BifKSpLaR",
        "FnmK2mvskaMzzHWUMEiAm6r1WGsW34xFphf5Xv9J115B",
    ]
    
    platform_indexer_endpoint = "https://test-avax-base.api.aleph.cloud/"
    # dict: solana = SOL, base = BASE, avalanche = AVAX
    platform_indexer_chains = Dict[str, str] = {
        "solana": "SOL",
        "base": "BASE",
        "avalanche": "AVAX",
    }

    crn_inactivity_threshold_days: int = 90

    class Config:
        env_file = ".env"


settings = Settings()
