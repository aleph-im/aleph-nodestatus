#!/usr/bin/env python3
"""
Diagnose why a CCN or CRN node was added/removed from the corechannel aggregate.

This script traces all events related to a specific node hash through:
1. Local LevelDB databases (messages, balances, erc20, scores)
2. Aleph network API queries
3. Ethereum blockchain balance checks

Usage:
    python scripts/diagnose_node.py <node_hash>
    python scripts/diagnose_node.py <node_hash> --owner <owner_address>
    python scripts/diagnose_node.py <node_hash> --db-path ./database
"""

import argparse
import asyncio
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project to path
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

# Try to import web3 for Ethereum balance fetching
try:
    from web3 import Web3
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False

# Try to import project modules
try:
    from aleph_nodestatus.storage import Storage
    from aleph_nodestatus.settings import settings
    STORAGE_AVAILABLE = True
except ImportError:
    STORAGE_AVAILABLE = False
    print("Warning: Could not import project modules. Some features disabled.")


# Constants
API_SERVER = "https://api2.aleph.im"
ALEPH_TOKEN_CONTRACT = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
DECIMALS = 10 ** 18
NODE_THRESHOLD = 199999  # ALEPH required to run a node
STAKING_THRESHOLD = 9999  # ALEPH required to stake
DEFAULT_ETH_RPC = "https://mainnet.infura.io/v3/REDACTED_API_KEY"


def get_web3(eth_rpc: Optional[str] = None) -> Optional["Web3"]:
    """Get Web3 instance for Ethereum queries."""
    if not WEB3_AVAILABLE:
        return None
    rpc_url = eth_rpc or DEFAULT_ETH_RPC
    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))
        if w3.is_connected():
            return w3
    except Exception as e:
        print(f"Warning: Web3 initialization failed: {e}")
    return None


def get_token_contract_abi() -> List[Dict]:
    """Load ALEPH ERC20 ABI."""
    abi_path = PROJECT_ROOT / "src" / "aleph_nodestatus" / "abi" / "ALEPHERC20.json"
    if abi_path.exists():
        with open(abi_path) as f:
            return json.load(f)
    # Minimal ABI
    return [{"constant": True, "inputs": [{"name": "_owner", "type": "address"}],
             "name": "balanceOf", "outputs": [{"name": "balance", "type": "uint256"}], "type": "function"}]


def fetch_eth_balance(w3: "Web3", address: str) -> float:
    """Fetch current ALEPH token balance for an address."""
    try:
        contract = w3.eth.contract(
            address=w3.to_checksum_address(ALEPH_TOKEN_CONTRACT),
            abi=get_token_contract_abi()
        )
        checksum_addr = w3.to_checksum_address(address)
        balance_wei = contract.functions.balanceOf(checksum_addr).call()
        return balance_wei / DECIMALS
    except Exception as e:
        print(f"  Error fetching balance for {address}: {e}")
        return -1


def fetch_message_from_api(item_hash: str) -> Optional[Dict]:
    """Fetch a specific message from the Aleph API."""
    url = f"{API_SERVER}/api/v0/messages/{item_hash}"
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            return json.loads(response.read().decode())
    except Exception as e:
        print(f"  API error for {item_hash}: {e}")
        return None


def fetch_messages_by_ref(ref: str, limit: int = 100) -> List[Dict]:
    """Fetch messages that reference a specific hash."""
    url = f"{API_SERVER}/api/v0/messages.json?refs={ref}&limit={limit}"
    try:
        with urllib.request.urlopen(url, timeout=15) as response:
            data = json.loads(response.read().decode())
            return data.get("messages", [])
    except Exception as e:
        print(f"  API error fetching refs: {e}")
        return []


def fetch_messages_by_content_hash(item_hash: str) -> List[Dict]:
    """Fetch POST messages that might contain this hash in content."""
    # Search for corechan-operation messages
    url = f"{API_SERVER}/api/v0/messages.json?msgType=POST&content.type=corechan-operation&limit=200"
    try:
        with urllib.request.urlopen(url, timeout=30) as response:
            data = json.loads(response.read().decode())
            messages = data.get("messages", [])
            # Filter for messages referencing our hash
            return [m for m in messages if m.get("item_hash") == item_hash or m.get("content", {}).get("ref") == item_hash]
    except Exception as e:
        print(f"  API error: {e}")
        return []


def format_timestamp(ts) -> str:
    """Format a timestamp to readable string."""
    if isinstance(ts, (int, float)):
        return datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S")
    return str(ts)


async def search_local_db(db: "Storage", node_hash: str, owner: Optional[str] = None, db_name: str = "") -> List[Dict]:
    """Search local database for entries related to a node."""
    results = []

    # Determine prefixes to search based on database type
    if db_name == "messages":
        prefixes = [
            "POST_mainnet_corechan-operation,amend",
            "POST_mainnet_corechan-operation",
            "item"  # fallback
        ]
    elif db_name == "scores":
        prefixes = [
            "POST_mainnet_aleph-scoring-scores",
            "item"
        ]
    else:
        prefixes = ["item"]

    for prefix in prefixes:
        try:
            async for key, data in db.retrieve_entries(prefix=prefix):
                # Handle nested message structure
                msg = data.get("message", data)

                # Check if this entry relates to our node
                is_related = False
                relation_type = None

                item_hash = msg.get("item_hash", "")
                content = msg.get("content", {})
                ref = content.get("ref", "") if isinstance(content, dict) else ""
                content_address = content.get("address", "") if isinstance(content, dict) else ""
                inner_content = content.get("content", {}) if isinstance(content, dict) else {}
                action = inner_content.get("action", "") if isinstance(inner_content, dict) else ""

                if item_hash == node_hash:
                    is_related = True
                    relation_type = f"CREATE ({action or 'item_hash match'})"
                elif ref == node_hash:
                    is_related = True
                    relation_type = f"REFERENCE ({action})"
                elif owner and content_address and content_address.lower() == owner.lower():
                    is_related = True
                    relation_type = f"OWNER ACTION ({action})"

                if is_related:
                    results.append({
                        "key": key,
                        "prefix": prefix,
                        "relation": relation_type,
                        "data": data
                    })
        except Exception as e:
            print(f"    Error searching with prefix '{prefix}': {e}")

    return results


async def search_all_local_dbs(db_path: str, node_hash: str, owner: Optional[str] = None) -> Dict[str, List[Dict]]:
    """Search all local databases for entries related to a node."""
    results = {}
    db_names = ["messages", "balances", "erc20", "scores"]

    for db_name in db_names:
        try:
            db = Storage(db_path, db_name)
            print(f"  Searching {db_name} database...")
            entries = await search_local_db(db, node_hash, owner, db_name=db_name)
            if entries:
                results[db_name] = entries
            db.close()
        except Exception as e:
            print(f"  Error reading {db_name} database: {e}")

    return results


async def check_db_sync_status(db_path: str) -> Dict[str, Any]:
    """Check the sync status of local databases."""
    status = {}

    # Check messages DB
    try:
        db = Storage(db_path, "messages")
        prefix = "POST_mainnet_corechan-operation,amend"

        first_key = None
        last_key = None
        count = 0

        async for key, data in db.retrieve_entries(prefix=prefix):
            count += 1
            if first_key is None:
                first_key = key
            last_key = key

        if first_key and last_key:
            # Keys are formatted as: {height}_{time}_{hash}
            first_height = int(first_key.split("_")[0])
            last_height = int(last_key.split("_")[0])
            first_time = int(first_key.split("_")[1])
            last_time = int(last_key.split("_")[1])

            status["messages"] = {
                "count": count,
                "first_height": first_height,
                "last_height": last_height,
                "first_time": format_timestamp(first_time),
                "last_time": format_timestamp(last_time),
                "time_range_days": (last_time - first_time) / 86400
            }
        else:
            status["messages"] = {"count": 0, "error": "No entries found with expected prefix"}

        db.close()
    except Exception as e:
        status["messages"] = {"error": str(e)}

    return status


def analyze_node_creation(message: Dict) -> Dict[str, Any]:
    """Analyze a node creation message."""
    content = message.get("content", {})
    msg_content = message.get("message", {}).get("content", content)

    if isinstance(msg_content, dict):
        inner_content = msg_content.get("content", {})
        action = inner_content.get("action", "unknown")
        details = inner_content.get("details", {})
        address = msg_content.get("address", "")
    else:
        action = "unknown"
        details = {}
        address = ""

    return {
        "action": action,
        "owner": address,
        "details": details,
        "time": message.get("time") or message.get("message", {}).get("time"),
        "status": message.get("status", "unknown"),
        "forgotten_by": message.get("forgotten_by", [])
    }


def print_section(title: str):
    """Print a section header."""
    print(f"\n{'=' * 70}")
    print(f" {title}")
    print('=' * 70)


def print_subsection(title: str):
    """Print a subsection header."""
    print(f"\n{'-' * 50}")
    print(f" {title}")
    print('-' * 50)


async def diagnose_node(node_hash: str, owner: Optional[str] = None, db_path: Optional[str] = None,
                        eth_rpc: Optional[str] = None, verbose: bool = False):
    """Run full diagnosis for a node."""

    print_section(f"DIAGNOSING NODE: {node_hash}")

    # 1. Fetch the original create-node message from API
    print_subsection("1. FETCHING NODE CREATION MESSAGE FROM API")

    create_msg = fetch_message_from_api(node_hash)
    if create_msg:
        analysis = analyze_node_creation(create_msg)
        print(f"  Status: {analysis['status']}")
        print(f"  Action: {analysis['action']}")
        print(f"  Owner: {analysis['owner']}")
        print(f"  Time: {format_timestamp(analysis['time'])}")

        if analysis['forgotten_by']:
            print(f"  ⚠️  FORGOTTEN BY: {analysis['forgotten_by']}")
            print("     This message was explicitly forgotten on the network!")

        if analysis['details']:
            print(f"  Details:")
            for k, v in analysis['details'].items():
                if k not in ['authorized']:  # Skip large fields
                    print(f"    {k}: {v}")

        # Use owner from message if not provided
        if not owner and analysis['owner']:
            owner = analysis['owner']
            print(f"\n  Using owner from message: {owner}")
    else:
        print("  ❌ Could not fetch message from API")
        print("     This could mean:")
        print("     - The message was never created")
        print("     - The message hash is incorrect")
        print("     - API connectivity issues")

    # 2. Fetch messages that reference this node
    print_subsection("2. FETCHING MESSAGES REFERENCING THIS NODE")

    ref_messages = fetch_messages_by_ref(node_hash)
    if ref_messages:
        print(f"  Found {len(ref_messages)} messages referencing this node:\n")

        # Sort by time
        ref_messages.sort(key=lambda m: m.get("time", 0))

        for msg in ref_messages:
            msg_time = format_timestamp(msg.get("time", 0))
            msg_hash = msg.get("item_hash", "?")[:16]
            msg_type = msg.get("content", {}).get("type", "?")
            msg_action = msg.get("content", {}).get("content", {}).get("action", "?")
            msg_address = msg.get("content", {}).get("address", "?")

            action_icon = {
                "stake": "📥",
                "unstake": "📤",
                "drop-node": "🗑️",
                "link": "🔗",
                "unlink": "⛓️‍💥",
                "amend": "✏️"
            }.get(msg_action, "❓")

            print(f"  {action_icon} [{msg_time}] {msg_action}")
            print(f"     Hash: {msg_hash}... | Address: {msg_address}")

            if msg_action == "drop-node":
                print(f"     ⚠️  THIS IS A DROP-NODE MESSAGE - explains removal!")
    else:
        print("  No messages found referencing this node")

    # 3. Check current Ethereum balance
    if owner:
        print_subsection("3. CHECKING CURRENT ETHEREUM BALANCE")

        w3 = get_web3(eth_rpc)
        if w3:
            balance = fetch_eth_balance(w3, owner)
            if balance >= 0:
                status_icon = "✅" if balance >= NODE_THRESHOLD else "❌"
                print(f"  Owner: {owner}")
                print(f"  Current Balance: {balance:,.2f} ALEPH")
                print(f"  Required for CCN: {NODE_THRESHOLD:,} ALEPH")
                print(f"  Status: {status_icon} {'SUFFICIENT' if balance >= NODE_THRESHOLD else 'INSUFFICIENT'}")

                if balance < NODE_THRESHOLD:
                    print(f"\n  ⚠️  Owner balance is below threshold!")
                    print(f"     This could explain why the node was removed.")
                    print(f"     Deficit: {NODE_THRESHOLD - balance:,.2f} ALEPH")
            else:
                print(f"  ❌ Could not fetch balance")
        else:
            print("  ⚠️  Web3 not available, skipping balance check")

    # 4. Search local databases
    if db_path or STORAGE_AVAILABLE:
        print_subsection("4. CHECKING LOCAL DATABASE STATUS")

        actual_db_path = db_path or (settings.db_path if STORAGE_AVAILABLE else "./database")
        print(f"  Database path: {actual_db_path}")

        if os.path.exists(actual_db_path):
            # First check sync status
            sync_status = await check_db_sync_status(actual_db_path)
            if "messages" in sync_status:
                msg_status = sync_status["messages"]
                if "error" not in msg_status:
                    print(f"\n  Messages DB sync status:")
                    print(f"    Total entries: {msg_status['count']:,}")
                    print(f"    Height range: {msg_status['first_height']:,} -> {msg_status['last_height']:,}")
                    print(f"    Time range: {msg_status['first_time']} -> {msg_status['last_time']}")
                    print(f"    Coverage: {msg_status['time_range_days']:.1f} days")

                    # Check if the node's creation time falls within the synced range
                    if create_msg:
                        node_time = create_msg.get("message", {}).get("time", 0)
                        if node_time:
                            node_time_str = format_timestamp(node_time)
                            print(f"\n    Node created: {node_time_str}")
                            # Compare with sync range
                            first_time = int(msg_status['first_time'].split()[0].replace("-", ""))
                            last_time_str = msg_status['last_time']
                            print(f"    ⚠️  Check if node creation time falls within synced range!")
                else:
                    print(f"    Error: {msg_status['error']}")

        print_subsection("4b. SEARCHING LOCAL DATABASES")

        if os.path.exists(actual_db_path):
            local_results = await search_all_local_dbs(actual_db_path, node_hash, owner)

            if local_results:
                for db_name, entries in local_results.items():
                    print(f"\n  📁 {db_name.upper()} ({len(entries)} entries):")
                    for entry in entries[:10]:  # Limit output
                        print(f"     Key: {entry['key']}")
                        print(f"     Relation: {entry['relation']}")
                        if verbose:
                            print(f"     Data: {json.dumps(entry['data'], indent=6, default=str)[:500]}")
                    if len(entries) > 10:
                        print(f"     ... and {len(entries) - 10} more entries")
            else:
                print("  ❌ No entries found in local databases")
                print("     This could mean:")
                print("     - The node was created before the local DB started tracking")
                print("     - The local DB is missing data")
                print("     - The pagination change caused messages to be missed")
        else:
            print(f"  ❌ Database path does not exist: {actual_db_path}")

    # 5. Summary and diagnosis
    print_subsection("5. DIAGNOSIS SUMMARY")

    issues = []

    if create_msg:
        analysis = analyze_node_creation(create_msg)
        if analysis['forgotten_by']:
            issues.append("MESSAGE FORGOTTEN: The create-node message was explicitly forgotten on the network")
        if analysis['status'] != "processed":
            issues.append(f"MESSAGE STATUS: Status is '{analysis['status']}' (not 'processed')")
    else:
        issues.append("MESSAGE NOT FOUND: Create-node message not found on API")

    # Check for drop-node in references
    drop_node_found = any(
        msg.get("content", {}).get("content", {}).get("action") == "drop-node"
        for msg in ref_messages
    )
    if drop_node_found:
        issues.append("DROP-NODE FOUND: An explicit drop-node message was sent for this node")

    if owner and WEB3_AVAILABLE:
        w3 = get_web3(eth_rpc)
        if w3:
            balance = fetch_eth_balance(w3, owner)
            if 0 <= balance < NODE_THRESHOLD:
                issues.append(f"LOW BALANCE: Owner balance ({balance:,.2f}) is below threshold ({NODE_THRESHOLD:,})")

    if issues:
        print("  ⚠️  POTENTIAL ISSUES FOUND:\n")
        for i, issue in enumerate(issues, 1):
            print(f"     {i}. {issue}")
    else:
        print("  ✅ No obvious issues found")
        print("     The node removal might be due to:")
        print("     - Temporary balance dip during processing")
        print("     - Missing messages in local database")
        print("     - Pagination window edge case")

    print("\n" + "=" * 70)


def main():
    parser = argparse.ArgumentParser(
        description="Diagnose why a CCN/CRN node was added or removed",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python scripts/diagnose_node.py e86557e9bdcd2db723dbf0cf1763e0ff5a93834690d9279fc245c8650d2a3869
  python scripts/diagnose_node.py <hash> --owner 0xB76FD0102b84FF3583071ea8b85FfE2B3f4D086f
  python scripts/diagnose_node.py <hash> --db-path ./database --verbose
        """
    )
    parser.add_argument("node_hash", help="The node hash (item_hash) to diagnose")
    parser.add_argument("--owner", help="Owner address (auto-detected if not provided)")
    parser.add_argument("--db-path", help="Path to local LevelDB databases")
    parser.add_argument("--eth-rpc", help="Ethereum RPC endpoint")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show verbose output")

    args = parser.parse_args()

    asyncio.run(diagnose_node(
        node_hash=args.node_hash,
        owner=args.owner,
        db_path=args.db_path,
        eth_rpc=args.eth_rpc,
        verbose=args.verbose
    ))


if __name__ == "__main__":
    main()
