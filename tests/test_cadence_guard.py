from aleph_nodestatus.credit_distribution import should_skip_run


def test_should_skip_when_within_interval():
    assert should_skip_run(last_end=1000.0, now=1500.0,
                           min_interval=1000.0, force=False) is True


def test_should_not_skip_when_past_interval():
    assert should_skip_run(last_end=1000.0, now=3000.0,
                           min_interval=1000.0, force=False) is False


def test_force_overrides_skip():
    assert should_skip_run(last_end=1000.0, now=1500.0,
                           min_interval=1000.0, force=True) is False


def test_no_last_end_does_not_skip():
    assert should_skip_run(last_end=None, now=1500.0,
                           min_interval=1000.0, force=False) is False
