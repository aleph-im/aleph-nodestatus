#!/usr/bin/env bash
#
# Bootstraps a local Anvil fork so the nodestatus fork-mode dry-runs
# (--fork-rpc on extract / distribute) have everything they need to
# exercise the real on-chain code paths against forked state, without
# requiring the production signing keys.
#
# Concretely it covers the two walls operators hit immediately after
# starting Anvil:
#
#   1. extract: process() is gated by ADMIN_ROLE on the AlephPayment-
#      Processor. The signer must hold that role on the fork or every
#      tx reverts with AccessControlUnauthorizedAccount.
#
#   2. extract: the processor needs USDC / ETH / ALEPH balances to
#      actually sweep — otherwise extract just prints `skipped=
#      zero_balance` for every token and you've verified nothing.
#
#   3. distribute: batchTransfer needs the signer to hold ALEPH or
#      the transfer reverts with "Balance not enough" before the
#      audit can read anything.
#
# Everything happens inside the Anvil fork via `cast rpc anvil_*`
# (impersonateAccount + setBalance) and impersonated transfers. No
# mainnet writes, no real pkeys required.
#
# Usage:
#   scripts/fork-bootstrap.sh --mode <extract|distribute|all> \
#                             [--signer <addr>] \
#                             [--rpc <url>] \
#                             [--admin <addr>] \
#                             [--dist-recipient <addr>]
#
# Defaults (all overridable via flags or env vars of the same name):
#   --signer            0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266
#                       (Anvil dev account #0, pkey ac0974be…ff80)
#   --rpc               http://localhost:8545
#   --admin             0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E
#                       (settings.payment_processor_admin_address)
#   --dist-recipient    0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25
#                       (settings.distribution_recipient)
#
# Three operator-facing scenarios:
#
#   A. "I don't have the production pkey — use Anvil's dev account":
#        scripts/fork-bootstrap.sh --mode all
#      Then run extract / distribute with -e
#      *_pkey=0xac0974bec39a17e36ba4a6b4d238ff944bacb478cbed5efcae784d7bf4f2ff80
#
#   B. "I have the real admin pkey, just want balances seeded":
#        scripts/fork-bootstrap.sh --mode extract \
#          --signer 0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E
#      (signer == admin → role grant is a no-op; only funding runs)
#
#   C. "I'm verifying with a throwaway address I generated":
#        scripts/fork-bootstrap.sh --mode all --signer 0xMY_TEST_ADDR
#
# Idempotent: rerunning the same mode against the same fork only
# re-funds and re-grants; nothing breaks if state was already there.

set -euo pipefail

# Defaults
MODE=""
SIGNER="${SIGNER:-0xf39Fd6e51aad88F6F4ce6aB8827279cffFb92266}"
ANVIL_RPC="${ANVIL_RPC:-http://localhost:8545}"
ADMIN="${ADMIN:-0xC870B0Ca4B3d65f33E2a3c732ab3cD2aE555b14E}"
DIST_RECIPIENT="${DIST_RECIPIENT:-0x3a5CC6aBd06B601f4654035d125F9DD2FC992C25}"

# Canonical addresses — these mirror src/aleph_nodestatus/settings.py.
# If the deployment changes, edit settings.py first, then this script.
PROCESSOR=0x6b55F32Ea969910838defd03746Ced5E2AE8cB8B
ALEPH_TOKEN=0x27702a26126e0B3702af63Ee09aC4d1A084EF628
USDC_TOKEN=0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48
# keccak256("ADMIN_ROLE")
ADMIN_ROLE=0xa49807205ce4d355092ef5a8a18f56e8913cf4a201fbe287825b095693c21775
# Circle's USDC treasury — used as a known USDC whale to impersonate
# when we need to credit the processor with USDC. Mainnet-only address.
USDC_WHALE=0x55FE002aefF02F77364de339a1292923A15844B8

# Amounts. Sized so a single extract run has meaningful balances to
# sweep but not so large that arithmetic overflows surface artificially.
USDC_FUND_AMOUNT=1000000000          # 1,000 USDC (6 decimals)
ETH_FUND_AMOUNT=0x56BC75E2D63100000  # 100 ETH in hex wei
ALEPH_FUND_AMOUNT=100000000000000000000000   # 100,000 ALEPH wei
ALEPH_TO_SIGNER=1000000000000000000000000    # 1,000,000 ALEPH wei (distribute)
ETH_GAS_BUFFER=0xDE0B6B3A7640000     # 1 ETH (for impersonated senders)

# Parse args
while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)            MODE="$2"; shift 2 ;;
    --signer)          SIGNER="$2"; shift 2 ;;
    --rpc)             ANVIL_RPC="$2"; shift 2 ;;
    --admin)           ADMIN="$2"; shift 2 ;;
    --dist-recipient)  DIST_RECIPIENT="$2"; shift 2 ;;
    -h|--help)
      sed -n '1,/^set -euo pipefail$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *) echo "Unknown flag: $1" >&2; exit 2 ;;
  esac
done

if [[ -z "$MODE" ]]; then
  echo "ERROR: --mode is required (extract | distribute | all)" >&2
  exit 2
fi
if [[ ! "$MODE" =~ ^(extract|distribute|all)$ ]]; then
  echo "ERROR: --mode must be one of: extract, distribute, all" >&2
  exit 2
fi
command -v cast >/dev/null || {
  echo "ERROR: \`cast\` not on PATH. Install Foundry: " \
       "curl -L https://foundry.paradigm.xyz | bash && foundryup" >&2
  exit 2
}

# Sanity: Anvil is up and responding
if ! cast block-number --rpc-url "$ANVIL_RPC" >/dev/null 2>&1; then
  echo "ERROR: Anvil not reachable at $ANVIL_RPC. Start it with:" >&2
  echo "  anvil --fork-url <your-archive-rpc> --chain-id 1 --host 0.0.0.0 --port 8545" >&2
  exit 2
fi

# Normalise addresses to lowercase for comparisons (cast accepts mixed case
# but =~ comparisons want canonical form).
to_lower() { echo "$1" | tr '[:upper:]' '[:lower:]'; }
SIGNER_LC=$(to_lower "$SIGNER")
ADMIN_LC=$(to_lower "$ADMIN")
DIST_LC=$(to_lower "$DIST_RECIPIENT")

echo "=== fork-bootstrap config ==="
echo "  mode            : $MODE"
echo "  rpc             : $ANVIL_RPC"
echo "  signer          : $SIGNER"
echo "  admin           : $ADMIN"
echo "  dist_recipient  : $DIST_RECIPIENT"
echo "  block (fork)    : $(cast block-number --rpc-url "$ANVIL_RPC")"
echo ""

# Generic helper: impersonate an address, ensure it has gas, run a
# `cast send` from it. Stops impersonation at the end regardless of
# success — leaves the fork in a clean state for the next caller.
send_as() {
  local from="$1"; shift
  cast rpc anvil_impersonateAccount "$from" --rpc-url "$ANVIL_RPC" >/dev/null
  cast rpc anvil_setBalance "$from" "$ETH_GAS_BUFFER" --rpc-url "$ANVIL_RPC" >/dev/null
  trap "cast rpc anvil_stopImpersonatingAccount $from --rpc-url $ANVIL_RPC >/dev/null 2>&1 || true" RETURN
  cast send --from "$from" --unlocked --rpc-url "$ANVIL_RPC" "$@" >/dev/null
}

bootstrap_extract() {
  echo "--- extract bootstrap ---"

  # 1. Grant ADMIN_ROLE to signer if it doesn't already have it. The
  #    only operator who can grant is the current admin (or whoever
  #    holds the DEFAULT_ADMIN role; for this contract admin == that).
  local has_role
  has_role=$(cast call "$PROCESSOR" "hasRole(bytes32,address)(bool)" \
    "$ADMIN_ROLE" "$SIGNER" --rpc-url "$ANVIL_RPC")
  if [[ "$has_role" == "true" ]]; then
    echo "  [skip] signer already has ADMIN_ROLE"
  else
    echo "  granting ADMIN_ROLE → $SIGNER (impersonating $ADMIN)"
    send_as "$ADMIN" "$PROCESSOR" \
      "grantRole(bytes32,address)" "$ADMIN_ROLE" "$SIGNER"
    local after
    after=$(cast call "$PROCESSOR" "hasRole(bytes32,address)(bool)" \
      "$ADMIN_ROLE" "$SIGNER" --rpc-url "$ANVIL_RPC")
    [[ "$after" == "true" ]] || {
      echo "ERROR: grantRole did not stick. Likely the configured" \
           "admin no longer holds ADMIN_ROLE on mainnet — find the" \
           "current admin via RoleGranted events on Etherscan." >&2
      exit 3
    }
  fi

  # 2. Fund the processor with USDC (via Circle's treasury whale).
  echo "  funding processor with USDC ($USDC_FUND_AMOUNT base units = 1,000 USDC)"
  send_as "$USDC_WHALE" "$USDC_TOKEN" \
    "transfer(address,uint256)" "$PROCESSOR" "$USDC_FUND_AMOUNT"

  # 3. Fund the processor with ETH (anvil_setBalance — no impersonation).
  echo "  funding processor with ETH ($ETH_FUND_AMOUNT wei = 100 ETH)"
  cast rpc anvil_setBalance "$PROCESSOR" "$ETH_FUND_AMOUNT" \
    --rpc-url "$ANVIL_RPC" >/dev/null

  # 4. Fund the processor with ALEPH (impersonate the distribution
  #    recipient who already holds ALEPH on mainnet — cheapest source).
  echo "  funding processor with ALEPH ($ALEPH_FUND_AMOUNT wei = 100,000 ALEPH)"
  send_as "$DIST_RECIPIENT" "$ALEPH_TOKEN" \
    "transfer(address,uint256)" "$PROCESSOR" "$ALEPH_FUND_AMOUNT"

  # 5. Summary read-back
  local usdc eth aleph
  usdc=$(cast call "$USDC_TOKEN" "balanceOf(address)(uint256)" "$PROCESSOR" --rpc-url "$ANVIL_RPC")
  eth=$(cast balance "$PROCESSOR" --rpc-url "$ANVIL_RPC")
  aleph=$(cast call "$ALEPH_TOKEN" "balanceOf(address)(uint256)" "$PROCESSOR" --rpc-url "$ANVIL_RPC")
  echo "  processor balances now:"
  echo "    USDC  : $usdc"
  echo "    ETH   : $eth wei"
  echo "    ALEPH : $aleph"
}

bootstrap_distribute() {
  echo "--- distribute bootstrap ---"

  # distribute needs the signer to hold ALEPH. If the signer IS the
  # configured distribution_recipient (i.e. operator passed the real
  # pkey path), no funding required — they already hold mainnet ALEPH
  # which the fork inherits.
  if [[ "$SIGNER_LC" == "$DIST_LC" ]]; then
    echo "  [skip] signer == distribution_recipient; mainnet ALEPH" \
         "balance is inherited by the fork. Nothing to fund."
  else
    local bal
    bal=$(cast call "$ALEPH_TOKEN" "balanceOf(address)(uint256)" "$SIGNER" --rpc-url "$ANVIL_RPC")
    echo "  signer current ALEPH balance: $bal"
    echo "  topping up signer with ALEPH ($ALEPH_TO_SIGNER wei = 1,000,000 ALEPH)"
    send_as "$DIST_RECIPIENT" "$ALEPH_TOKEN" \
      "transfer(address,uint256)" "$SIGNER" "$ALEPH_TO_SIGNER"
    local after
    after=$(cast call "$ALEPH_TOKEN" "balanceOf(address)(uint256)" "$SIGNER" --rpc-url "$ANVIL_RPC")
    echo "  signer ALEPH balance now    : $after"
  fi

  # ETH gas for the signer (idempotent — set to 100 ETH every time).
  echo "  ensuring signer has ETH for gas"
  cast rpc anvil_setBalance "$SIGNER" "$ETH_FUND_AMOUNT" --rpc-url "$ANVIL_RPC" >/dev/null
}

case "$MODE" in
  extract)    bootstrap_extract ;;
  distribute) bootstrap_distribute ;;
  all)        bootstrap_extract; echo ""; bootstrap_distribute ;;
esac

echo ""
echo "=== done — fork ready for nodestatus --fork-rpc=$ANVIL_RPC ==="
