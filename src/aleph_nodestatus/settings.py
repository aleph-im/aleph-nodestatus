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
    ethereum_blocks_per_day: int = 6500
    ethereum_batch_size: int = 200
    ethereum_block_width_big: int = 50000
    ethereum_block_width_small: int = 10000

    reward_start_height: int = 11519440
    reward_nodes_daily: int = 15000
    reward_stakers_daily_base: int = 15000
    
    staking_threshold: int = 10000
    node_threshold: int = 200000
    node_activation: int = 500000

    node_post_type: str = "corechan-operation"
    # staker_post_type: str = "corechan-delegation"
    filter_tag: str = "mainnet"

    class Config:
        env_file = '.env'


settings = Settings()
