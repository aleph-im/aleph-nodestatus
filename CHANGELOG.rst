=========
Changelog
=========

Version 0.2
===========

Migration
---------

- ``docker-compose.yml`` now defines two services: ``nodestatus-calc`` and
  ``nodestatus-dist``. Operators must create two env files,
  ``docker/.env.calc`` and ``docker/.env.dist``, each setting ``ethereum_pkey``
  to the appropriate role's key. See ``docker/.env.calc.example`` and
  ``docker/.env.dist.example`` for templates. The previous single
  ``docker/.env`` is no longer referenced by any service; existing operators
  should split its contents into the two new files and delete the old one.
  Single-instance dev/staging deployments can run
  ``docker compose up nodestatus-calc`` only.

- New ``nodestatus-distribute-credits`` flow:

  - Calls ``AlephPaymentProcessor.process()`` per token (USDC, ETH, ALEPH)
    with off-chain slippage-protected ``min_out``.
  - Adds the 6-month linearly-decaying wage subsidy
    (900,000 → 0 ALEPH/month over 6 months, split 1/3 CCN / 1/3 CRN / 1/3 stakers).
  - Holder-tier ``rewards[]`` second pass behind a feature flag
    (default on; ``--no-holder-tier`` disables).
  - ``--dry-run`` mode simulates ``process()`` via ``eth_call`` without
    broadcasting or posting.
  - Per-step feature flags (``--no-extract``, ``--no-wage``, ``--no-transfer``,
    ``--no-publish``, etc.).
  - Cadence guard (``credit_dist_min_interval_seconds``, default 10 days)
    with ``--force`` override.

Version 0.1
===========

- Feature A added
- FIX: nasty bug #1729 fixed
- add your changes here!
