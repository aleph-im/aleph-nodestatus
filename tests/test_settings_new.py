import pytest

from aleph_nodestatus.settings import settings


def test_payment_processor_settings_defaults():
    assert settings.payment_processor_address.lower() == \
        "0x6b55f32ea969910838defd03746ced5e2ae8cb8b"
    assert settings.distribution_recipient.lower() == \
        "0x3a5cc6abd06b601f4654035d125f9dd2fc992c25"
    assert settings.uniswap_v3_quoter_address.lower() == \
        "0x61ffe014ba17989e743c5f6cb21bf9697530b21e"
    assert settings.payment_processor_admin_address.lower() == \
        "0xc870b0ca4b3d65f33e2a3c732ab3cd2ae555b14e"


def test_token_list_default():
    syms = [s for s, _ in settings.process_tokens]
    assert syms == ["USDC", "ETH", "ALEPH"]


def test_wage_subsidy_defaults():
    assert settings.wage_initial_monthly_aleph == 900_000
    assert settings.wage_duration_months == 6
    assert abs(sum([settings.wage_ccn_share,
                    settings.wage_crn_share,
                    settings.wage_staker_share]) - 1.0) < 1e-9


def test_feature_flags_defaults():
    assert settings.credit_dist_credit_revenue_enabled is True
    assert settings.credit_dist_wage_subsidy_enabled is True
    assert settings.credit_dist_holder_tier_enabled is True
    assert settings.credit_dist_transfer_enabled is True
    assert settings.credit_dist_publish_enabled is True


def test_cadence_defaults():
    assert settings.credit_dist_min_interval_blocks == 10 * 7130
    assert settings.credit_dist_dust_threshold_aleph == 0.01
    assert settings.process_slippage_bps == 100
    assert settings.process_ttl_seconds == 1800


def test_process_ttl_seconds_rejects_above_3600():
    """M5: pydantic validator must reject TTL > 3600 (on-chain contract bound).

    The module-level ``settings = Settings()`` runs on reload, so the
    ValidationError surfaces there rather than in a separate Settings() call.
    """
    import importlib
    import os
    os.environ["process_ttl_seconds"] = "4000"
    try:
        from aleph_nodestatus import settings as settings_module
        with pytest.raises(Exception):  # pydantic.ValidationError or ValueError
            importlib.reload(settings_module)
    finally:
        del os.environ["process_ttl_seconds"]
        importlib.reload(settings_module)


def test_process_ttl_seconds_accepts_3600_exactly():
    import importlib
    import os
    os.environ["process_ttl_seconds"] = "3600"
    try:
        from aleph_nodestatus import settings as settings_module
        importlib.reload(settings_module)
        s = settings_module.Settings()
        assert s.process_ttl_seconds == 3600
    finally:
        del os.environ["process_ttl_seconds"]
        importlib.reload(settings_module)


def test_anti_mev_defaults():
    assert settings.extract_random_delay_max_seconds == 3540
    assert settings.extract_max_deviation_bps == 200
    assert settings.chainlink_max_age_seconds == 3600
    assert settings.uniswap_v3_factory_address.lower() == \
        "0x1f98431c8ad98523631ae4a59f267346ea31f984"
    assert settings.uniswap_v4_state_view_address.lower() == \
        "0x7ffe42c4a5deea5b0fec41c94c136cf115597227"
    # ETH sentinel and WETH both map to the ETH/USD feed.
    eth_usd = "0x5f4eC3Df9cbd43714FE2740f5E3616155c5b8419".lower()
    feeds = {k.lower(): v.lower() for k, v in
             settings.chainlink_usd_feeds.items()}
    assert feeds["0x0000000000000000000000000000000000000000"] == eth_usd
    assert feeds["0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"] == eth_usd
    assert feeds["0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"] == \
        "0x8fffffd4afb6115b954bd326cbe7b4ba576818f6".lower()


def test_credit_api_price_guard_settings_defaults():
    from aleph_nodestatus.settings import settings
    assert settings.extract_price_deviation_enabled is True
    assert settings.credit_api_url == "https://credit.aleph.im/api"
    assert settings.credit_api_blockchain == "ethereum"
    assert settings.credit_api_timeout_seconds == 10
    # threshold reused, unchanged
    assert settings.extract_max_deviation_bps == 200
