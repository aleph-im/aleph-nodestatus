#!/usr/bin/env python3
"""
Compare two corechannel aggregate messages to verify consistency.

Usage:
    python scripts/compare_corechannel.py <old_file.json> <new_file.json> --from-files
    python scripts/compare_corechannel.py <old_hash> <new_hash>
    python scripts/compare_corechannel.py <old_hash> --latest --address <sender_address>

Requirements:
    pip install web3
"""

import argparse
import json
import os
import sys
import urllib.request
import urllib.error
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, Optional, List
from concurrent.futures import ThreadPoolExecutor, as_completed

# Try to import web3 for Ethereum balance fetching
try:
    from web3 import Web3
    WEB3_AVAILABLE = True
except ImportError:
    WEB3_AVAILABLE = False
    print("Warning: web3 not installed. Ethereum balance fetching disabled.")
    print("Install with: pip install web3")


API_SERVER = "https://api2.aleph.im"
ALEPH_TOKEN_CONTRACT = "0x27702a26126e0B3702af63Ee09aC4d1A084EF628"
DECIMALS = 10 ** 18

# Default Infura endpoint
DEFAULT_ETH_RPC = os.environ.get("ETHEREUM_API_SERVER", "")


def get_web3(eth_rpc: Optional[str] = None) -> Optional["Web3"]:
    """Get Web3 instance for Ethereum queries."""
    if not WEB3_AVAILABLE:
        return None

    rpc_url = eth_rpc or DEFAULT_ETH_RPC

    try:
        w3 = Web3(Web3.HTTPProvider(rpc_url, request_kwargs={"timeout": 30}))

        if w3.is_connected():
            return w3
        else:
            print("Warning: Could not connect to Ethereum node")
            return None
    except Exception as e:
        print(f"Warning: Web3 initialization failed: {e}")
        return None


def get_token_contract_abi() -> List[Dict]:
    """Load ALEPH ERC20 ABI from project or use minimal ABI."""
    # Try to load from project
    abi_path = Path(__file__).resolve().parent.parent / "src" / "aleph_nodestatus" / "abi" / "ALEPHERC20.json"
    if abi_path.exists():
        with open(abi_path) as f:
            return json.load(f)

    # Minimal ABI for balanceOf
    return [
        {
            "constant": True,
            "inputs": [{"name": "_owner", "type": "address"}],
            "name": "balanceOf",
            "outputs": [{"name": "balance", "type": "uint256"}],
            "type": "function"
        }
    ]


def fetch_eth_balance(w3: "Web3", contract, address: str) -> float:
    """Fetch ALEPH token balance for an address."""
    try:
        checksum_addr = w3.to_checksum_address(address)
        balance_wei = contract.functions.balanceOf(checksum_addr).call()
        return balance_wei / DECIMALS
    except Exception as e:
        return -1  # Error indicator


def fetch_stakers_balances(w3: Optional["Web3"], staker_addresses: List[str]) -> Dict[str, float]:
    """Fetch balances for multiple staker addresses."""
    if not w3 or not staker_addresses:
        return {}

    try:
        contract = w3.eth.contract(
            address=w3.to_checksum_address(ALEPH_TOKEN_CONTRACT),
            abi=get_token_contract_abi()
        )
    except Exception as e:
        print(f"Warning: Could not create contract: {e}")
        return {}

    balances = {}

    # Fetch in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_addr = {
            executor.submit(fetch_eth_balance, w3, contract, addr): addr
            for addr in staker_addresses
        }

        for future in as_completed(future_to_addr):
            addr = future_to_addr[future]
            try:
                balances[addr] = future.result()
            except Exception:
                balances[addr] = -1

    return balances


def fetch_message_info(item_hash: str) -> Dict[str, Any]:
    """Fetch message status and timestamp from API."""
    url = f"{API_SERVER}/api/v0/messages/{item_hash}"
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read().decode())

            status = data.get("status", "unknown")

            # Extract timestamp
            msg = data.get("message", {})
            time_val = msg.get("time")

            if isinstance(time_val, (int, float)):
                dt = datetime.fromtimestamp(time_val)
                time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(time_val, str):
                # Parse ISO format
                try:
                    dt = datetime.fromisoformat(time_val.replace("Z", "+00:00"))
                    time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
                except:
                    time_str = time_val[:19] if time_val else "unknown"
            else:
                time_str = "unknown"

            return {
                "status": status,
                "time": time_str,
                "forgotten_by": data.get("forgotten_by", [])
            }
    except Exception as e:
        return {"status": "error", "time": str(e), "forgotten_by": []}


def fetch_messages_info_batch(hashes: List[str], max_workers: int = 10) -> Dict[str, Dict]:
    """Fetch message info for multiple hashes in parallel."""
    results = {}

    if not hashes:
        return results

    print(f"  Fetching info for {len(hashes)} messages...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_hash = {executor.submit(fetch_message_info, h): h for h in hashes}

        for future in as_completed(future_to_hash):
            item_hash = future_to_hash[future]
            try:
                results[item_hash] = future.result()
            except Exception as e:
                results[item_hash] = {"status": "error", "time": str(e), "forgotten_by": []}

    return results


def load_from_file(filepath: str) -> Optional[Dict]:
    """Load message content from a JSON file."""
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)

            # Handle different nested formats
            # Format 1: {message: {content: {content: {nodes: [...], resource_nodes: [...]}}}}
            if "message" in data:
                msg = data["message"]
                if "content" in msg:
                    content = msg["content"]
                    if "content" in content and "nodes" in content["content"]:
                        return content["content"]
                    if "nodes" in content:
                        return content

            # Format 2: {content: {content: {nodes: [...]}}}
            if "content" in data:
                content = data["content"]
                if "content" in content and "nodes" in content["content"]:
                    return content["content"]
                if "nodes" in content:
                    return content

            # Format 3: Direct {nodes: [...], resource_nodes: [...]}
            if "nodes" in data:
                return data

            print(f"Warning: Could not find nodes in {filepath}")
            print(f"Keys found: {list(data.keys())}")
            return data
    except Exception as e:
        print(f"Error loading file {filepath}: {e}")
        return None


def compare_nodes(old_nodes: list, new_nodes: list) -> Dict[str, Any]:
    """Compare node lists and return differences."""
    old_by_hash = {n["hash"]: n for n in old_nodes}
    new_by_hash = {n["hash"]: n for n in new_nodes}

    old_hashes = set(old_by_hash.keys())
    new_hashes = set(new_by_hash.keys())

    added = new_hashes - old_hashes
    removed = old_hashes - new_hashes
    common = old_hashes & new_hashes

    modified = []
    staker_diffs = []

    for node_hash in common:
        old_node = old_by_hash[node_hash]
        new_node = new_by_hash[node_hash]

        diffs = {}

        # Compare key fields
        fields_to_compare = [
            "status", "total_staked", "score", "decentralization",
            "performance", "owner", "reward", "locked", "name",
            "multiaddress", "address", "inactive_since", "has_bonus"
        ]

        for field in fields_to_compare:
            old_val = old_node.get(field)
            new_val = new_node.get(field)
            if old_val != new_val:
                diffs[field] = {"old": old_val, "new": new_val}

        # Compare stakers
        old_stakers = old_node.get("stakers", {})
        new_stakers = new_node.get("stakers", {})

        if old_stakers != new_stakers:
            old_staker_addrs = set(old_stakers.keys())
            new_staker_addrs = set(new_stakers.keys())

            staker_diff = {
                "node_hash": node_hash,
                "node_name": new_node.get("name", ""),
                "added_stakers": list(new_staker_addrs - old_staker_addrs),
                "removed_stakers": list(old_staker_addrs - new_staker_addrs),
                "changed_amounts": {}
            }

            for addr in old_staker_addrs & new_staker_addrs:
                if old_stakers[addr] != new_stakers[addr]:
                    staker_diff["changed_amounts"][addr] = {
                        "old": old_stakers[addr],
                        "new": new_stakers[addr],
                        "diff": new_stakers[addr] - old_stakers[addr]
                    }

            if staker_diff["added_stakers"] or staker_diff["removed_stakers"] or staker_diff["changed_amounts"]:
                staker_diffs.append(staker_diff)

        # Compare resource_nodes
        old_rnodes = set(old_node.get("resource_nodes", []))
        new_rnodes = set(new_node.get("resource_nodes", []))
        if old_rnodes != new_rnodes:
            diffs["resource_nodes"] = {
                "added": list(new_rnodes - old_rnodes),
                "removed": list(old_rnodes - new_rnodes)
            }

        if diffs:
            modified.append({
                "hash": node_hash,
                "name": new_node.get("name", ""),
                "differences": diffs
            })

    return {
        "added_nodes": list(added),
        "removed_nodes": list(removed),
        "modified_nodes": modified,
        "staker_differences": staker_diffs,
        "total_old": len(old_nodes),
        "total_new": len(new_nodes),
        "total_common": len(common)
    }


def compare_resource_nodes(old_rnodes: list, new_rnodes: list) -> Dict[str, Any]:
    """Compare resource node lists and return differences."""
    old_by_hash = {n["hash"]: n for n in old_rnodes}
    new_by_hash = {n["hash"]: n for n in new_rnodes}

    old_hashes = set(old_by_hash.keys())
    new_hashes = set(new_by_hash.keys())

    added = new_hashes - old_hashes
    removed = old_hashes - new_hashes
    common = old_hashes & new_hashes

    modified = []

    for node_hash in common:
        old_node = old_by_hash[node_hash]
        new_node = new_by_hash[node_hash]

        diffs = {}

        fields_to_compare = [
            "status", "parent", "score", "decentralization",
            "performance", "owner", "manager", "reward", "locked",
            "name", "address", "inactive_since"
        ]

        for field in fields_to_compare:
            old_val = old_node.get(field)
            new_val = new_node.get(field)
            if old_val != new_val:
                diffs[field] = {"old": old_val, "new": new_val}

        if diffs:
            modified.append({
                "hash": node_hash,
                "name": new_node.get("name", ""),
                "differences": diffs
            })

    return {
        "added_resource_nodes": list(added),
        "removed_resource_nodes": list(removed),
        "modified_resource_nodes": modified,
        "total_old": len(old_rnodes),
        "total_new": len(new_rnodes),
        "total_common": len(common)
    }


def calculate_totals(content: Dict) -> Dict[str, Any]:
    """Calculate summary statistics for a corechannel content."""
    nodes = content.get("nodes", [])
    resource_nodes = content.get("resource_nodes", [])

    total_staked = sum(n.get("total_staked", 0) for n in nodes)
    active_nodes = sum(1 for n in nodes if n.get("status") == "active")
    waiting_nodes = sum(1 for n in nodes if n.get("status") == "waiting")

    linked_rnodes = sum(1 for r in resource_nodes if r.get("status") == "linked")
    waiting_rnodes = sum(1 for r in resource_nodes if r.get("status") == "waiting")

    all_stakers = set()
    for node in nodes:
        all_stakers.update(node.get("stakers", {}).keys())

    return {
        "total_nodes": len(nodes),
        "active_nodes": active_nodes,
        "waiting_nodes": waiting_nodes,
        "total_resource_nodes": len(resource_nodes),
        "linked_resource_nodes": linked_rnodes,
        "waiting_resource_nodes": waiting_rnodes,
        "total_staked": total_staked,
        "unique_stakers": len(all_stakers)
    }


def print_comparison(old_content: Dict, new_content: Dict, old_label: str, new_label: str,
                     fetch_info: bool = True, w3: Optional["Web3"] = None):
    """Print a detailed comparison report."""
    print("\n" + "=" * 80)
    print("CORECHANNEL COMPARISON REPORT")
    print("=" * 80)

    # Build node lookup dictionaries for staker info
    old_nodes_by_hash = {n["hash"]: n for n in old_content.get("nodes", [])}
    new_nodes_by_hash = {n["hash"]: n for n in new_content.get("nodes", [])}

    # Summary statistics
    old_totals = calculate_totals(old_content)
    new_totals = calculate_totals(new_content)

    print(f"\n{'SUMMARY':^80}")
    print("-" * 80)
    print(f"{'Metric':<35} {'Old':>20} {'New':>20}")
    print("-" * 80)

    for key in old_totals:
        old_val = old_totals[key]
        new_val = new_totals[key]
        diff_marker = "" if old_val == new_val else " *"

        if isinstance(old_val, float):
            print(f"{key:<35} {old_val:>20,.2f} {new_val:>20,.2f}{diff_marker}")
        else:
            print(f"{key:<35} {old_val:>20,} {new_val:>20,}{diff_marker}")

    print("-" * 80)
    print("* indicates differences")

    # Compare nodes
    print(f"\n{'NODE COMPARISON':^80}")
    print("-" * 80)

    node_diff = compare_nodes(
        old_content.get("nodes", []),
        new_content.get("nodes", [])
    )

    # Fetch info for added/removed nodes
    added_info = {}
    removed_info = {}

    if fetch_info:
        if node_diff["added_nodes"]:
            added_info = fetch_messages_info_batch(node_diff["added_nodes"])
        if node_diff["removed_nodes"]:
            removed_info = fetch_messages_info_batch(node_diff["removed_nodes"])

    if node_diff["added_nodes"]:
        print(f"\nADDED NODES ({len(node_diff['added_nodes'])}):")

        # Collect all staker addresses for added nodes to fetch balances
        all_staker_addrs = set()
        for h in node_diff["added_nodes"]:
            node = new_nodes_by_hash.get(h, {})
            stakers = node.get("stakers", {})
            all_staker_addrs.update(stakers.keys())

        # Fetch real balances from Ethereum
        staker_balances = {}
        if w3 and all_staker_addrs:
            print(f"  Fetching Ethereum balances for {len(all_staker_addrs)} staker addresses...")
            staker_balances = fetch_stakers_balances(w3, list(all_staker_addrs))

        for h in node_diff["added_nodes"]:
            info = added_info.get(h, {})
            status = info.get("status", "?")
            time_str = info.get("time", "?")

            # Get staker info from new_content
            node = new_nodes_by_hash.get(h, {})
            stakers = node.get("stakers", {})
            n_stakers = len(stakers)
            node_name = node.get("name", "")

            # Calculate real balance from Ethereum
            if staker_balances:
                real_balance = sum(staker_balances.get(addr, 0) for addr in stakers.keys() if staker_balances.get(addr, 0) > 0)
                balance_str = f"Real Balance: {real_balance:,.2f} ALEPH"
            else:
                balance_str = "Real Balance: N/A"

            print(f"  + {h} [{status}] {time_str} [Stakers: {n_stakers}. {balance_str}]")
            if node_name:
                print(f"    Name: {node_name}")

    if node_diff["removed_nodes"]:
        print(f"\nREMOVED NODES ({len(node_diff['removed_nodes'])}):")

        # Collect all staker addresses (and owners) for removed nodes to fetch balances
        all_staker_addrs = set()
        for h in node_diff["removed_nodes"]:
            node = old_nodes_by_hash.get(h, {})
            stakers = node.get("stakers", {})
            all_staker_addrs.update(stakers.keys())
            # Also add the owner - important for checking if removal was due to balance drop
            owner = node.get("owner")
            if owner:
                all_staker_addrs.add(owner)

        # Fetch real balances from Ethereum
        staker_balances = {}
        if w3 and all_staker_addrs:
            print(f"  Fetching Ethereum balances for {len(all_staker_addrs)} staker addresses...")
            staker_balances = fetch_stakers_balances(w3, list(all_staker_addrs))

        for h in node_diff["removed_nodes"]:
            info = removed_info.get(h, {})
            status = info.get("status", "?")
            time_str = info.get("time", "?")
            forgotten = " (forgotten)" if info.get("forgotten_by") else ""

            # Get staker info from old_content
            node = old_nodes_by_hash.get(h, {})
            stakers = node.get("stakers", {})
            n_stakers = len(stakers)
            node_name = node.get("name", "")
            owner = node.get("owner", "")

            # Calculate real balance from Ethereum
            if staker_balances:
                real_balance = sum(staker_balances.get(addr, 0) for addr in stakers.keys() if staker_balances.get(addr, 0) > 0)
                balance_str = f"Real Balance: {real_balance:,.2f} ALEPH"
            else:
                balance_str = "Real Balance: N/A"

            print(f"  - {h} [{status}] {time_str} [Stakers: {n_stakers}. {balance_str}]{forgotten}")
            if node_name:
                print(f"    Name: {node_name}")
            if owner:
                # Also show owner's balance if we have it
                owner_bal = staker_balances.get(owner, -1) if staker_balances else -1
                if owner_bal >= 0:
                    print(f"    Owner: {owner} (Balance: {owner_bal:,.2f} ALEPH)")

    if node_diff["modified_nodes"]:
        print(f"\nMODIFIED NODES ({len(node_diff['modified_nodes'])}):")
        for mod in node_diff["modified_nodes"][:15]:
            print(f"\n  Node: {mod['name'] or mod['hash'][:16]}...")
            for field, diff in mod["differences"].items():
                if field == "resource_nodes":
                    if diff.get("added"):
                        print(f"    {field}: +{len(diff['added'])} resource nodes")
                    if diff.get("removed"):
                        print(f"    {field}: -{len(diff['removed'])} resource nodes")
                elif field in ["total_staked", "score", "decentralization", "performance"]:
                    old_v = diff['old'] if diff['old'] is not None else 0
                    new_v = diff['new'] if diff['new'] is not None else 0
                    print(f"    {field}: {old_v:.4f} -> {new_v:.4f}")
                else:
                    print(f"    {field}: {diff['old']} -> {diff['new']}")
        if len(node_diff["modified_nodes"]) > 15:
            print(f"\n  ... and {len(node_diff['modified_nodes']) - 15} more modified nodes")

    # Staker differences
    if node_diff["staker_differences"]:
        print(f"\n{'STAKER CHANGES':^80}")
        print("-" * 80)
        print(f"Nodes with staker changes: {len(node_diff['staker_differences'])}")

        for sd in node_diff["staker_differences"][:5]:
            print(f"\n  Node: {sd['node_name'] or sd['node_hash'][:16]}...")
            if sd["added_stakers"]:
                print(f"    Added stakers: {len(sd['added_stakers'])}")
            if sd["removed_stakers"]:
                print(f"    Removed stakers: {len(sd['removed_stakers'])}")
            if sd["changed_amounts"]:
                print(f"    Changed amounts: {len(sd['changed_amounts'])}")
                for addr, change in list(sd["changed_amounts"].items())[:3]:
                    print(f"      {addr[:16]}...: {change['old']:.2f} -> {change['new']:.2f} ({change['diff']:+.2f})")

        if len(node_diff["staker_differences"]) > 5:
            print(f"\n  ... and {len(node_diff['staker_differences']) - 5} more nodes with staker changes")

    # Compare resource nodes
    print(f"\n{'RESOURCE NODE COMPARISON':^80}")
    print("-" * 80)

    rnode_diff = compare_resource_nodes(
        old_content.get("resource_nodes", []),
        new_content.get("resource_nodes", [])
    )

    # Fetch info for added/removed resource nodes
    added_rnode_info = {}
    removed_rnode_info = {}

    if fetch_info:
        if rnode_diff["added_resource_nodes"]:
            added_rnode_info = fetch_messages_info_batch(rnode_diff["added_resource_nodes"])
        if rnode_diff["removed_resource_nodes"]:
            removed_rnode_info = fetch_messages_info_batch(rnode_diff["removed_resource_nodes"])

    if rnode_diff["added_resource_nodes"]:
        print(f"\nADDED RESOURCE NODES ({len(rnode_diff['added_resource_nodes'])}):")
        for h in rnode_diff["added_resource_nodes"]:
            info = added_rnode_info.get(h, {})
            status = info.get("status", "?")
            time_str = info.get("time", "?")
            print(f"  + {h} [{status}] {time_str}")

    if rnode_diff["removed_resource_nodes"]:
        print(f"\nREMOVED RESOURCE NODES ({len(rnode_diff['removed_resource_nodes'])}):")
        for h in rnode_diff["removed_resource_nodes"]:
            info = removed_rnode_info.get(h, {})
            status = info.get("status", "?")
            time_str = info.get("time", "?")
            forgotten = " (forgotten)" if info.get("forgotten_by") else ""
            print(f"  - {h} [{status}] {time_str}{forgotten}")

    if rnode_diff["modified_resource_nodes"]:
        print(f"\nMODIFIED RESOURCE NODES ({len(rnode_diff['modified_resource_nodes'])}):")
        for mod in rnode_diff["modified_resource_nodes"][:15]:
            print(f"\n  Resource Node: {mod['name'] or mod['hash'][:16]}...")
            for field, diff in mod["differences"].items():
                if field in ["score", "decentralization", "performance"]:
                    old_v = diff['old'] if diff['old'] is not None else 0
                    new_v = diff['new'] if diff['new'] is not None else 0
                    print(f"    {field}: {old_v:.4f} -> {new_v:.4f}")
                else:
                    print(f"    {field}: {diff['old']} -> {diff['new']}")
        if len(rnode_diff["modified_resource_nodes"]) > 15:
            print(f"\n  ... and {len(rnode_diff['modified_resource_nodes']) - 15} more")

    # Final verdict
    print(f"\n{'VERDICT':^80}")
    print("=" * 80)

    is_identical = (
        not node_diff["added_nodes"] and
        not node_diff["removed_nodes"] and
        not node_diff["modified_nodes"] and
        not node_diff["staker_differences"] and
        not rnode_diff["added_resource_nodes"] and
        not rnode_diff["removed_resource_nodes"] and
        not rnode_diff["modified_resource_nodes"]
    )

    if is_identical:
        print("IDENTICAL: Both messages contain the same data.")
    else:
        changes = []
        if node_diff["added_nodes"] or node_diff["removed_nodes"]:
            changes.append(f"node count changed ({node_diff['total_old']} -> {node_diff['total_new']})")
        if node_diff["modified_nodes"]:
            changes.append(f"{len(node_diff['modified_nodes'])} nodes modified")
        if node_diff["staker_differences"]:
            changes.append(f"{len(node_diff['staker_differences'])} nodes have staker changes")
        if rnode_diff["added_resource_nodes"] or rnode_diff["removed_resource_nodes"]:
            changes.append(f"resource node count changed ({rnode_diff['total_old']} -> {rnode_diff['total_new']})")
        if rnode_diff["modified_resource_nodes"]:
            changes.append(f"{len(rnode_diff['modified_resource_nodes'])} resource nodes modified")

        print("DIFFERENCES FOUND:")
        for change in changes:
            print(f"  - {change}")

    print("=" * 80)


def main():
    parser = argparse.ArgumentParser(description="Compare two corechannel aggregate messages")
    parser.add_argument("old", help="Old message file path or hash")
    parser.add_argument("new", help="New message file path or hash")
    parser.add_argument("--from-files", action="store_true", help="Load from JSON files instead of fetching")
    parser.add_argument("--no-fetch", action="store_true", help="Don't fetch additional info for added/removed nodes")
    parser.add_argument("--eth-rpc", help="Ethereum RPC endpoint for balance queries (e.g., https://mainnet.infura.io/v3/YOUR_KEY)")
    parser.add_argument("--no-eth", action="store_true", help="Skip Ethereum balance fetching")
    parser.add_argument("--output", "-o", help="Save comparison to JSON file")

    args = parser.parse_args()

    # Initialize Web3 for Ethereum balance queries
    w3 = None
    if not args.no_eth and WEB3_AVAILABLE:
        w3 = get_web3(args.eth_rpc)
        if w3:
            print(f"Connected to Ethereum (block: {w3.eth.block_number})")
        else:
            print("Ethereum connection not available. Balance fetching disabled.")

    # Load old content
    if args.from_files:
        print(f"Loading: {args.old}")
        old_content = load_from_file(args.old)
        if not old_content:
            print(f"Could not load: {args.old}")
            sys.exit(1)
    else:
        print("Fetching from API not implemented in this version. Use --from-files")
        sys.exit(1)

    # Load new content
    if args.from_files:
        print(f"Loading: {args.new}")
        new_content = load_from_file(args.new)
        if not new_content:
            print(f"Could not load: {args.new}")
            sys.exit(1)
    else:
        print("Fetching from API not implemented in this version. Use --from-files")
        sys.exit(1)

    # Run comparison
    print_comparison(old_content, new_content, args.old, args.new, fetch_info=not args.no_fetch, w3=w3)

    # Optionally save to file
    if args.output:
        result = {
            "old_label": args.old,
            "new_label": args.new,
            "old_totals": calculate_totals(old_content),
            "new_totals": calculate_totals(new_content),
            "node_diff": compare_nodes(old_content.get("nodes", []), new_content.get("nodes", [])),
            "resource_node_diff": compare_resource_nodes(
                old_content.get("resource_nodes", []),
                new_content.get("resource_nodes", [])
            )
        }
        with open(args.output, 'w') as f:
            json.dump(result, f, indent=2)
        print(f"\nDetailed comparison saved to: {args.output}")


if __name__ == "__main__":
    main()
