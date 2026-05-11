from aleph_nodestatus.credit_distribution import should_skip_run


def test_should_skip_when_within_interval():
    assert should_skip_run(last_end_height=1000, current_height=1500,
                           min_interval_blocks=1000, force=False) is True


def test_should_not_skip_when_past_interval():
    assert should_skip_run(last_end_height=1000, current_height=3000,
                           min_interval_blocks=1000, force=False) is False


def test_force_overrides_skip():
    assert should_skip_run(last_end_height=1000, current_height=1500,
                           min_interval_blocks=1000, force=True) is False


def test_no_last_end_does_not_skip():
    assert should_skip_run(last_end_height=None, current_height=1500,
                           min_interval_blocks=1000, force=False) is False
