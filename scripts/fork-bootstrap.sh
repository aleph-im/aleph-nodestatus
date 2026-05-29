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
#   scripts/fork-bootstrap.sh --mode <extract|distribute|all|none> \
#                             [--signer <addr>] \
#                             [--rpc <url>] \
#                             [--admin <addr>] \
#                             [--dist-recipient <addr>] \
#                             [--ethereum-api-server <url>] \
#                             [--env-file <path>] \
#                             [--no-anvil] \
#                             [--anvil-port <N>] \
#                             [--anvil-block-number <N>]
#
# Modes:
#   extract    — grant ADMIN_ROLE + fund processor (USDC/ETH/ALEPH)
#   distribute — top up signer with ALEPH
#   all        — extract + distribute
#   none       — verify Anvil is up, do NOT mutate fork state.
#                Use this for true mainnet-state verification: the
#                signer MUST be the real admin / distribution
#                recipient with the corresponding production pkey
#                (otherwise extract reverts with
#                AccessControlUnauthorizedAccount or distribute reverts
#                with "Balance not enough" — which is exactly the
#                behaviour you'd see on mainnet today, so the audit
#                output reflects production reality).
#
# Anvil lifecycle:
#   By default the script LAUNCHES Anvil itself, reading the upstream
#   RPC URL from `ethereum_api_server` the same way extract/distribute
#   do (CLI flag → env var → docker/.env.extract → docker/.env.dist →
#   ./.env). Anvil runs in the background and survives this script's
#   exit; the PID + log path are printed so the operator can kill it
#   afterwards. If Anvil is already running on the target port the
#   script detects it and reuses it.
#
#   Pass --no-anvil to opt out (e.g. you already manage Anvil with
#   another supervisor); the script then expects it to be reachable
#   at --rpc (default http://localhost:8545) and only does the
#   bootstrap.
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
# Optional explicit grantor for ADMIN_ROLE. If empty, the script
# auto-discovers via Ownable.owner() and the configured admin. See
# `find_role_grantor` below for the resolution chain.
ADMIN_GRANTOR="${ADMIN_GRANTOR:-}"

# Anvil-launch defaults (overridable via CLI flags below).
ETHEREUM_API_SERVER_CLI=""
ENV_FILE_CLI=""
LAUNCH_ANVIL=1
ANVIL_PORT=8545
ANVIL_BLOCK=""
ANVIL_LOG="${ANVIL_LOG:-/tmp/anvil-fork-bootstrap.log}"
# When the script launches Anvil itself, the default is to keep it
# alive in the foreground after bootstrap (Ctrl-C → clean shutdown).
# Set DETACH=1 (or pass --detach) for the old behaviour where the
# script exits leaving Anvil orphaned in the background.
DETACH="${DETACH:-0}"
# PID of the launched Anvil. Empty when --no-anvil or when we reused
# an existing instance — the EXIT trap distinguishes both.
ANVIL_PID=""

# Project root — resolved relative to this script so the env-file
# scanner finds `docker/.env.extract` regardless of cwd.
PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"

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
    --mode)                  MODE="$2"; shift 2 ;;
    --signer)                SIGNER="$2"; shift 2 ;;
    --rpc)                   ANVIL_RPC="$2"; shift 2 ;;
    --admin)                 ADMIN="$2"; shift 2 ;;
    --admin-grantor)         ADMIN_GRANTOR="$2"; shift 2 ;;
    --dist-recipient)        DIST_RECIPIENT="$2"; shift 2 ;;
    --ethereum-api-server)   ETHEREUM_API_SERVER_CLI="$2"; shift 2 ;;
    --env-file)              ENV_FILE_CLI="$2"; shift 2 ;;
    --no-anvil)              LAUNCH_ANVIL=0; shift ;;
    --anvil-port)            ANVIL_PORT="$2"; shift 2 ;;
    --anvil-block-number)    ANVIL_BLOCK="$2"; shift 2 ;;
    --detach)                DETACH=1; shift ;;
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
if [[ ! "$MODE" =~ ^(extract|distribute|all|none)$ ]]; then
  echo "ERROR: --mode must be one of: extract, distribute, all, none" >&2
  exit 2
fi
command -v cast >/dev/null || {
  echo "ERROR: \`cast\` not on PATH. Install Foundry: " \
       "curl -L https://foundry.paradigm.xyz | bash && foundryup" >&2
  exit 2
}

# Resolve the upstream RPC URL using the same priority chain as the
# nodestatus Settings loader: explicit CLI flag → process env var →
# explicit --env-file → docker/.env.extract → docker/.env.dist →
# project-root .env. The first source that defines a non-empty
# `ethereum_api_server` wins. Echoes the resolved URL on stdout (so
# callers can `RPC=$(... | tail -1)`) and the source on stderr.
resolve_upstream_rpc() {
  local v=""
  if [[ -n "$ETHEREUM_API_SERVER_CLI" ]]; then
    echo "[upstream RPC ← --ethereum-api-server flag]" >&2
    echo "$ETHEREUM_API_SERVER_CLI"
    return 0
  fi
  if [[ -n "${ETHEREUM_API_SERVER:-}" ]]; then
    echo "[upstream RPC ← \$ETHEREUM_API_SERVER env var]" >&2
    echo "$ETHEREUM_API_SERVER"
    return 0
  fi
  # Helper: grep one assignment out of a .env file, strip optional
  # surrounding quotes, ignore comments / leading whitespace. Same
  # tolerance as pydantic-settings reads the file with.
  read_env_var() {
    local f="$1" key="$2"
    [[ -r "$f" ]] || return 1
    local line
    line=$(grep -E "^[[:space:]]*${key}=" "$f" | head -1) || return 1
    [[ -n "$line" ]] || return 1
    local raw="${line#*=}"
    raw="${raw%$'\r'}"
    raw="${raw#\"}"; raw="${raw%\"}"
    raw="${raw#\'}"; raw="${raw%\'}"
    [[ -n "$raw" ]] && { echo "$raw"; return 0; }
    return 1
  }
  if [[ -n "$ENV_FILE_CLI" ]]; then
    if v=$(read_env_var "$ENV_FILE_CLI" "ethereum_api_server"); then
      echo "[upstream RPC ← $ENV_FILE_CLI]" >&2
      echo "$v"; return 0
    fi
  fi
  for f in "$PROJECT_ROOT/docker/.env.extract" \
           "$PROJECT_ROOT/docker/.env.dist" \
           "$PROJECT_ROOT/.env"; do
    if v=$(read_env_var "$f" "ethereum_api_server"); then
      echo "[upstream RPC ← $f]" >&2
      echo "$v"; return 0
    fi
  done
  return 1
}

# Launch Anvil in the background, fork from $UPSTREAM_RPC. Idempotent:
# if the target port is already serving (e.g. another Anvil instance
# is up), it's reused instead of starting a duplicate.
launch_anvil() {
  local upstream="$1"
  local port="$2"
  local block="${3:-}"
  local target_rpc="http://localhost:${port}"

  if cast block-number --rpc-url "$target_rpc" >/dev/null 2>&1; then
    echo "  [reuse] Anvil already responding on port $port"
    ANVIL_RPC="$target_rpc"
    return 0
  fi

  command -v anvil >/dev/null || {
    echo "ERROR: \`anvil\` not on PATH. Install Foundry: " \
         "curl -L https://foundry.paradigm.xyz | bash && foundryup" >&2
    exit 2
  }

  # Sanity-ping the upstream so we fail fast if the URL is wrong /
  # unreachable, rather than waiting 30s for Anvil to time out.
  local latest_upstream
  if ! latest_upstream=$(cast block-number --rpc-url "$upstream" 2>&1); then
    echo "ERROR: upstream RPC not reachable at $upstream:" >&2
    echo "  $latest_upstream" >&2
    exit 3
  fi
  echo "  upstream ok (latest block $latest_upstream)"

  local cmd=(anvil --fork-url "$upstream" --chain-id 1
                   --host 0.0.0.0 --port "$port")
  [[ -n "$block" ]] && cmd+=(--fork-block-number "$block")
  echo "  launching: ${cmd[*]} (log: $ANVIL_LOG)"
  : > "$ANVIL_LOG"
  nohup "${cmd[@]}" > "$ANVIL_LOG" 2>&1 &
  ANVIL_PID=$!
  echo "  Anvil PID: $ANVIL_PID — kill with: kill $ANVIL_PID"

  # Wait up to ~30s for the JSON-RPC port to come alive. Anvil
  # typically takes 1–4s to fork; we poll at 0.5s and abort early
  # if the process died (e.g. bad upstream URL slipped through the
  # cheap ping).
  local i=0
  until cast block-number --rpc-url "$target_rpc" >/dev/null 2>&1; do
    i=$((i + 1))
    if (( i > 60 )); then
      echo "ERROR: Anvil did not come up within 30s. Tail of log:" >&2
      tail -30 "$ANVIL_LOG" >&2
      exit 3
    fi
    if ! kill -0 "$ANVIL_PID" 2>/dev/null; then
      echo "ERROR: Anvil process exited before listening. Log:" >&2
      tail -30 "$ANVIL_LOG" >&2
      exit 3
    fi
    sleep 0.5
  done
  ANVIL_RPC="$target_rpc"
  echo "  Anvil ready at $ANVIL_RPC"
}

if (( LAUNCH_ANVIL == 1 )); then
  echo "=== anvil launch ==="
  if ! UPSTREAM_RPC=$(resolve_upstream_rpc); then
    echo "ERROR: could not resolve upstream RPC URL." >&2
    echo "  Pass --ethereum-api-server <url>, set " \
         "ETHEREUM_API_SERVER, or define ethereum_api_server in one " \
         "of: docker/.env.extract, docker/.env.dist, .env" >&2
    exit 2
  fi
  launch_anvil "$UPSTREAM_RPC" "$ANVIL_PORT" "$ANVIL_BLOCK"
  echo ""
fi

# Sanity: whatever Anvil we're targeting (just-launched or reused or
# externally-managed via --no-anvil), confirm it's responding before
# the bootstrap mutates anything.
if ! cast block-number --rpc-url "$ANVIL_RPC" >/dev/null 2>&1; then
  echo "ERROR: Anvil not reachable at $ANVIL_RPC." >&2
  if (( LAUNCH_ANVIL == 0 )); then
    echo "  --no-anvil was set; start Anvil yourself, e.g.:" >&2
    echo "  anvil --fork-url <url> --chain-id 1 --host 0.0.0.0 --port 8545" >&2
  fi
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
echo "  upstream RPC    : ${UPSTREAM_RPC:-(external — anvil managed elsewhere)}"
echo "  anvil endpoint  : $ANVIL_RPC"
echo "  signer          : $SIGNER"
echo "  admin           : $ADMIN"
echo "  dist_recipient  : $DIST_RECIPIENT"
echo "  block (fork)    : $(cast block-number --rpc-url "$ANVIL_RPC")"
echo ""

# Find an address that can grant `role` on the processor. Order:
#
#   1. --admin-grantor flag, if the operator supplied one (and it
#      actually holds the role's admin)
#   2. Ownable.owner() — by far the most common case on OZ contracts
#      where the deployer holds both ownership and DEFAULT_ADMIN_ROLE
#   3. The configured operational admin ($ADMIN — may or may not be
#      the role-admin; on AlephPaymentProcessor it's the operator
#      that calls process(), which holds ADMIN_ROLE but NOT
#      DEFAULT_ADMIN_ROLE)
#
# Each candidate is `hasRole`-checked against `getRoleAdmin(role)`
# before being returned; a wrong guess is rejected silently and the
# next candidate is tried.
find_role_grantor() {
  local role="$1"
  local role_admin
  role_admin=$(cast call "$PROCESSOR" "getRoleAdmin(bytes32)(bytes32)" \
    "$role" --rpc-url "$ANVIL_RPC" 2>/dev/null) || {
      echo "ERROR: getRoleAdmin call failed — is $PROCESSOR really" \
           "the processor contract on this fork?" >&2
      return 1
    }
  echo "  role-admin of target role: $role_admin" >&2

  local candidates=()
  [[ -n "$ADMIN_GRANTOR" ]] && candidates+=("$ADMIN_GRANTOR")

  # Try Ownable.owner(). Wrapped in `|| true` because the call reverts
  # on non-Ownable contracts — we just fall through to the next
  # candidate then.
  local owner=""
  owner=$(cast call "$PROCESSOR" "owner()(address)" \
    --rpc-url "$ANVIL_RPC" 2>/dev/null || true)
  if [[ -n "$owner" && "$owner" != "0x0000000000000000000000000000000000000000" ]]; then
    candidates+=("$owner")
  fi

  # Last fallback: the configured operational admin. On most projects
  # this *is* the role-admin too; on AlephPaymentProcessor it isn't,
  # which is exactly why this auto-discovery exists.
  candidates+=("$ADMIN")

  for c in "${candidates[@]}"; do
    local has
    has=$(cast call "$PROCESSOR" "hasRole(bytes32,address)(bool)" \
      "$role_admin" "$c" --rpc-url "$ANVIL_RPC" 2>/dev/null) || has="false"
    if [[ "$has" == "true" ]]; then
      echo "  found grantor: $c (holds $role_admin)" >&2
      echo "$c"
      return 0
    else
      echo "  [skip] $c does not hold role-admin $role_admin" >&2
    fi
  done

  cat >&2 <<EOF
ERROR: no candidate holds the role-admin for the target role.
  Find the actual holder via Etherscan:
    1. Open $PROCESSOR on etherscan.io
    2. Read Contract → getRoleAdmin(<target_role>) → $role_admin
    3. Filter the contract's "RoleGranted" events by role = $role_admin
       and grab the most recent grantee that wasn't subsequently revoked.
    4. Re-run with --admin-grantor <that_address>.
EOF
  return 1
}

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
    local grantor
    grantor=$(find_role_grantor "$ADMIN_ROLE") || exit 3
    echo "  granting ADMIN_ROLE → $SIGNER (impersonating $grantor)"
    send_as "$grantor" "$PROCESSOR" \
      "grantRole(bytes32,address)" "$ADMIN_ROLE" "$SIGNER"
    local after
    after=$(cast call "$PROCESSOR" "hasRole(bytes32,address)(bool)" \
      "$ADMIN_ROLE" "$SIGNER" --rpc-url "$ANVIL_RPC")
    [[ "$after" == "true" ]] || {
      echo "ERROR: grantRole did not stick even though $grantor was" \
           "reported to hold the role-admin. The tx likely reverted" \
           "for an unrelated reason — re-run with verbose Anvil logs" \
           "(tail -f $ANVIL_LOG) to see the revert reason." >&2
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
  none)
    # Mainnet-state verification: explicitly do NOT mutate fork state.
    # The fork is left exactly as it was at the forked block — same
    # roles, same balances, same allowances.
    echo "--- mode=none: no bootstrap (mainnet-state verification) ---"
    echo "  fork state left untouched."
    echo "  the signer must hold ADMIN_ROLE / ALEPH balance on its OWN" \
         "(use the real production pkey via -e *_pkey=…), otherwise" \
         "extract or distribute will revert exactly as they would on" \
         "mainnet right now."
    # Soft heuristic: warn if the user is about to combine `--mode none`
    # with the default Anvil dev address. The signer almost never has
    # the right role/balance in that combination, so the run is
    # guaranteed to revert — usually that's an oversight, not intent.
    DEFAULT_ANVIL="0xf39fd6e51aad88f6f4ce6ab8827279cfffb92266"
    if [[ "$SIGNER_LC" == "$DEFAULT_ANVIL" ]]; then
      echo ""
      echo "  WARNING: signer is Anvil dev account #0 AND --mode=none."
      echo "  This combination has no realistic path through extract or"
      echo "  distribute — Anvil's dev address has no ADMIN_ROLE and no"
      echo "  ALEPH balance on mainnet. Either pass --signer with the"
      echo "  real admin / distribution_recipient, or pick a mode that"
      echo "  actually bootstraps."
    fi
    ;;
esac

echo ""
echo "=== bootstrap done — fork ready for nodestatus --fork-rpc=$ANVIL_RPC ==="

# If we launched Anvil ourselves and the operator didn't ask for the
# detach-then-exit behaviour, hold the script open so Anvil's lifetime
# matches the script's lifetime. The trap converts Ctrl-C (or SIGTERM
# from the orchestrator) into a clean Anvil shutdown — no orphaned
# processes, no stale port bindings the next run has to detect.
if [[ -n "$ANVIL_PID" ]] && (( DETACH == 0 )); then
  cleanup_anvil() {
    # Idempotent — both INT/TERM and EXIT may fire on a normal Ctrl-C.
    if [[ -n "${ANVIL_PID:-}" ]] && kill -0 "$ANVIL_PID" 2>/dev/null; then
      echo ""
      echo "Stopping Anvil (PID $ANVIL_PID) …"
      kill "$ANVIL_PID" 2>/dev/null || true
      # Give it 5s to drain; SIGKILL if it lingers.
      for _ in 1 2 3 4 5; do
        kill -0 "$ANVIL_PID" 2>/dev/null || break
        sleep 1
      done
      kill -9 "$ANVIL_PID" 2>/dev/null || true
    fi
  }
  trap cleanup_anvil INT TERM EXIT
  echo ""
  echo "Anvil keeps running at $ANVIL_RPC (log: $ANVIL_LOG)."
  echo "Press Ctrl-C in this terminal to stop it."
  echo "Run your nodestatus verification in another terminal, e.g.:"
  echo "  docker compose -f docker/docker-compose.yml run --rm --no-deps \\"
  echo "    -e ethereum_api_server=\"http://host.docker.internal:$ANVIL_PORT\" \\"
  echo "    --entrypoint /usr/local/bin/nodestatus-extract-credits \\"
  echo "    nodestatus-extract --dry-run --fork-rpc=http://host.docker.internal:$ANVIL_PORT -v"
  # `wait` returns when Anvil exits (Ctrl-C, oom, …) — the trap fires
  # on the way out, so even an abnormal Anvil death is cleaned up
  # consistently.
  wait "$ANVIL_PID" 2>/dev/null || true
elif [[ -n "$ANVIL_PID" && $DETACH -eq 1 ]]; then
  echo ""
  echo "DETACH=1 / --detach — leaving Anvil running in the background."
  echo "  PID:    $ANVIL_PID"
  echo "  log:    $ANVIL_LOG"
  echo "  stop:   kill $ANVIL_PID"
fi
