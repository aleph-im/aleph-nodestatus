import pytest

from aleph_nodestatus.distribution import compute_score_multiplier


def test_compute_score_multiplier():
    t = compute_score_multiplier
    assert t(0) == 0
    assert t(1) == 1

    assert t(0.0) == 0
    assert t(0.1) == 0
    assert t(0.2) == 0
    assert t(0.3) == pytest.approx(0.16666666666666663)
    assert t(0.4) == pytest.approx(0.33333333333333337)
    assert t(0.5) == 0.5
    assert t(0.6) == pytest.approx(0.6666666666666666)
    assert t(0.7) == pytest.approx(0.8333333333333333)
    assert t(0.8) == 1
    assert t(0.9) == 1
    assert t(1.0) == 1


def test_score_multiplier_boundary():
    """Score just above 0.2 should give a tiny positive multiplier."""
    assert compute_score_multiplier(0.2001) > 0
    assert compute_score_multiplier(0.1999) == 0


def test_score_multiplier_clamped_input():
    """Scores outside [0,1] should still return sensible values."""
    assert compute_score_multiplier(-0.5) == 0
    assert compute_score_multiplier(1.5) == 1
