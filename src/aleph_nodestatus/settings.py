from typing import Dict, List

from pydantic import field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "aleph.im swapper"
    admin_email: str = "hello@aleph.im"
    # account_pkey: str = Field(..., env='ACCT_PKEY')
    # account_type: str = Field(..., env='ACCT_TYPE')
    aleph_channel: str = "TEST"
    aleph_api_server: str = "https://api.aleph.im"
    aleph_testnet_api_server: str = "https://api.twentysix.testnet.network"
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
    ethereum_block_width_big: int = 10000
    ethereum_block_width_small: int = 200

    ethereum_sablier_contract: str = "0xCD18eAa163733Da39c232722cBC4E8940b1D8888"
    ethereum_sablier_min_height: int = 13245838

    balances_platforms: List[str] = ["ALEPH_ETH_SABLIER"]
    balances_senders: List[str] = ["0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10"]

    scores_senders: List[str] = ["0x4D52380D3191274a04846c89c069E6C3F2Ed94e4", "0x4Ec8b55e73F5f32118a90B8FD555706bD5dd42e7"]
    scores_post_type: str = "aleph-scoring-scores"
    
    scores_discard_periods: List[List[int]] = [[23579375, 23631694]]

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
    balances_filter_tags: List[str] = ["0xCD18eAa163733Da39c232722cBC4E8940b1D8888"]
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

    platform_indexer_endpoint: str = "https://test-avax-base.api.aleph.cloud/"
    # dict: solana = SOL, base = BASE, avalanche = AVAX
    platform_indexer_chains: Dict[str, str] = {
        # "solana": "SOL",
        "base": "BASE",
        "avalanche": "AVAX",
    }
    platform_indexer_ignored_addresses: List[str] = [
    ]

    voucher_indexer_endpoint: str = "https://vouchers.api.2n6.io"

    # Owner of the `aleph_credit_expense` posts as seen by `/posts.json`.
    # The delegated signer (0x6aeaEE...) used to publish on behalf of this
    # account is accepted by `/messages.json` filters but not by `/posts.json`,
    # which only indexes the materialized post's owner.
    credit_expense_sender: str = "0x2E4454fAD1906c0Ce6e45cBFA05cE898Ac3AC1dC"
    status_sender: str = "0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10"

    # Credit distribution shares (storage, 95% total)
    credit_storage_ccn_share: float = 0.75
    credit_storage_staker_share: float = 0.20

    # Credit distribution shares (execution, 95% total)
    credit_execution_crn_share: float = 0.60
    credit_execution_ccn_share: float = 0.15
    credit_execution_staker_share: float = 0.20

    # Dev fund (not distributed, tracked for accounting)
    credit_dev_fund_share: float = 0.05

    crn_inactivity_threshold_days: int = 90
    crn_inactivity_cutoff_height: int = 20840959

    db_path: str = "./database"

    # === AlephPaymentProcessor ===
    payment_processor_address: str = "0x6b55f32ea969910838defd03746ced5e2ae8cb8b"
    payment_processor_admin_pkey: str = ""
    payment_processor_admin_address: str = "0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E"
    distribution_recipient: str = "0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25"

    # Tokens to process, in order. address(0) for ETH.
    process_tokens: List[tuple] = [
        ("USDC", "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
        ("ETH",  "0x0000000000000000000000000000000000000000"),
        ("ALEPH","0x27702a26126e0B3702af63Ee09aC4d1A084EF628"),
    ]

    # Slippage / quoter
    uniswap_v3_quoter_address: str = "0x61fFE014bA17989E743c5F6cB21bF9697530B21e"
    uniswap_v4_quoter_address: str = "0x52f0e24d1c21c8a0cb1e5a5dd6198556bd9e1203"
    uniswap_v2_router_address: str = "0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D"
    process_slippage_bps: int = 100          # 1%; see docs/operations/slippage.md
    process_ttl_seconds: int = 1800
    process_gas_ceiling: int = 1_500_000

    @field_validator("process_ttl_seconds")
    @classmethod
    def _ttl_within_contract_bound(cls, v):
        # AlephPaymentProcessor's process() enforces ttl <= 3600 on-chain.
        if v > 3600:
            raise ValueError(
                f"process_ttl_seconds must be <= 3600 (contract bound), got {v}"
            )
        if v <= 0:
            raise ValueError(f"process_ttl_seconds must be positive, got {v}")
        return v

    # === Wage subsidy ===
    wage_start_date: str = "2026-05-01T00:00:00+00:00"
    wage_duration_months: int = 6
    wage_initial_monthly_aleph: int = 900_000
    wage_ccn_share: float = 1/3
    wage_crn_share: float = 1/3
    wage_staker_share: float = 1/3

    # === Cadence & filtering ===
    credit_dist_min_interval_blocks: int = 10 * 7130
    credit_dist_dust_threshold_aleph: float = 0.01

    # Right-edge finality margin: cap `end_height` at `current_block - N` so
    # that by the time we compute, expenses whose end_date ∈ [start_time, end_time]
    # have already been published. Default sized for mainnet ETH (~12s/block):
    # 500 blocks ≈ 100 min, comfortably above the measured p99 execution
    # publish_lag of ~22 min plus one full cadence cycle (~60 min).
    credit_dist_safety_blocks: int = 500

    # Require at least one confirmed `aleph_credit_expense` post whose
    # `expense.end_date > end_time` as evidence that the publisher has
    # progressed past the period. Default: abort the run if absent (the
    # operator can downgrade to a warning via the CLI flag).
    credit_dist_require_expense_witness: bool = True

    # Per-request total timeout for the Aleph HTTP API. The default aiohttp
    # 5-minute cap is too tight: AGGREGATE+date-filtered first-page queries
    # over a multi-week window have been observed to take longer than that
    # before returning. Raising to 10 minutes gives the API room without
    # leaving a stuck run waiting forever.
    aleph_http_timeout_seconds: int = 600

    # Page size for Aleph API message/post iterators. The SDK default (200)
    # produces multi-MB responses for the corechannel `status_sender`
    # aggregates that this distributor scans — large enough that a single
    # page can exceed the request timeout. A smaller value trades extra
    # round-trips for a much higher chance the API can answer in time.
    aleph_http_page_size: int = 20

    # Cap on total ALEPH a single --act run can distribute. Aborts before
    # any balance check or transfer if an upstream bug produces inflated
    # rewards. Default sized at 2x the maximum monthly wage subsidy.
    credit_dist_max_total_aleph: float = 2 * 900_000  # 1.8M

    # === Feature flags ===
    credit_dist_credit_revenue_enabled: bool = True
    credit_dist_wage_subsidy_enabled: bool   = True
    credit_dist_holder_tier_enabled: bool    = True
    credit_dist_transfer_enabled: bool       = True
    credit_dist_publish_enabled: bool        = True

    # === Anti-MEV (Phase 1) ===
    extract_random_delay_max_seconds: int = 3540
    extract_max_deviation_bps: int        = 200

    # === Credit-API price guard ===
    # Output-deviation guard for the extract swap path. Compares the
    # quoter's expected_out against the Credit-API multi-source USD-implied
    # output. When enabled and the API is unavailable, the token is skipped
    # for that run (fail-closed). See
    # docs/specs/2026-06-01-credit-api-price-guard-design.md.
    extract_price_deviation_enabled: bool = True
    credit_api_url: str                   = "https://credit.aleph.im/api/v0"
    credit_api_blockchain: str            = "ethereum"
    credit_api_timeout_seconds: int       = 10

    # Auto-sizing of the swap input to stay under a self-impact ceiling.
    # When > 0 (in bps), the extractor probes the configured quoter at a
    # small-size "unit" amount and at the candidate swap amount, derives
    # the implied price impact, and bisects downward until the impact
    # fits within the threshold (or no amount above the per-token
    # `--min-amount` floor fits, in which case the token is skipped).
    # The leftover stays in the contract for the next cron cycle, giving
    # arbitrage bots time to rebalance the pool. 0 disables the search;
    # 100 = 1% impact ceiling, sized to absorb USDC/ETH pool depth on
    # the configured Uniswap paths. CLI flag `--max-price-impact-bps`
    # overrides for one-off ops runs.
    extract_max_price_impact_bps: int     = 100

    # Per-token floor on the swap amount (human units, decimals-aware).
    # If the extractable amount drops below this for a given token, the
    # cron skips it that cycle so the gas cost doesn't dominate the
    # value moved. Floor reasoning: at typical mainnet gas a process()
    # call costs ~$3-10, so a ~$100-value floor caps gas overhead at
    # ~10%. CLI `--min-amount SYMBOL=N` overrides per token (per-run,
    # not persistent). Override the whole map in .env if you change the
    # token list.
    extract_default_min_amounts: Dict[str, str] = {
        "USDC":  "100",
        "ETH":   "0.025",
        "ALEPH": "500",
    }

    # === Fork-verified dry-run ===
    # Optional URL of a local mainnet fork (typically `anvil --fork-url …`).
    # When set, `extract-credits --dry-run` upgrades to full sign+broadcast
    # against the fork, audits each receipt against the on-chain
    # `TokenPaymentsProcessed` event, and hard-asserts that the realized
    # ALEPH output is within `extract_max_reconcile_delta_bps` of the
    # quoter's `expected_out`. CLI flag `--fork-rpc` overrides this value.
    # MUST be a loopback/host.docker.internal URL — the CLI refuses other
    # hosts to prevent accidentally signing mainnet broadcasts.
    extract_fork_rpc: str                  = ""
    extract_max_reconcile_delta_bps: int   = 200

    # Same fork mode for `distribute-credits --dry-run`. Reconciliation
    # for distribute is per-recipient: each ERC20 Transfer event from the
    # batchTransfer receipt is matched against the calculated reward
    # amount for that recipient. There's no Uniswap involved in
    # distribute, so the natural default tolerance is 0 bps — every wei
    # must match. Raise only if you knowingly accept rounding drift.
    distribute_fork_rpc: str                  = ""
    distribute_max_reconcile_delta_bps: int   = 0

    model_config = SettingsConfigDict(env_file=".env")


settings = Settings()


# Runtime mode for publishing (can be switched via CLI flags)
class PublishMode:
    """Controls which API server is used for publishing data."""
    _use_testnet: bool = False

    @classmethod
    def set_testnet(cls, enabled: bool = True):
        """Enable or disable testnet mode for publishing."""
        cls._use_testnet = enabled

    @classmethod
    def is_testnet(cls) -> bool:
        """Check if testnet mode is enabled."""
        return cls._use_testnet

    @classmethod
    def get_publish_api_server(cls) -> str:
        """Get the API server URL for publishing based on current mode."""
        if cls._use_testnet:
            return settings.aleph_testnet_api_server
        return settings.aleph_api_server
