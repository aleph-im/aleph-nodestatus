import sys

import pytest

from aleph_nodestatus.distribution import compute_score_multiplier
from aleph_nodestatus.scores import process_scores_history
from aleph_nodestatus.settings import settings


def test_compute_score_multiplier():
    t = compute_score_multiplier
    assert t(0) == 0
    assert t(1) == 1

    assert t(0.) == 0
    assert t(0.1) == 0
    assert t(0.2) == 0
    assert t(0.3) == 0.16666666666666663
    assert t(0.4) == 0.33333333333333337
    assert t(0.5) == 0.5
    assert t(0.6) == 0.6666666666666666
    assert t(0.7) == 0.8333333333333333
    assert t(0.8) == 1
    assert t(0.9) == 1
    assert t(1.0) == 1
