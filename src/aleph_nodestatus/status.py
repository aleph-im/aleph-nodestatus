import asyncio
import random
from collections import deque
from urllib.parse import urlparse
from multiaddr import Multiaddr

from aleph_nodestatus.monitored import process_balances_history

from .erc20 import DECIMALS, process_contract_history
from .messages import get_aleph_account, process_message_history, set_status
from .settings import settings
from .utils import merge

NODE_AMT = settings.node_threshold * DECIMALS
STAKING_AMT = settings.staking_threshold * DECIMALS
ACTIVATION_AMT = settings.node_activation * DECIMALS
MAX_LINKED = settings.node_max_linked

EDITABLE_FIELDS = [
    "name",
    "multiaddress",
    "address",
    "picture",
    "banner",
    "description",
    "reward",
    "stream_reward",
    "manager",
    "authorized",
    "locked",
    "registration_url",
    "terms_and_conditions",
]


async def prepare_items(item_type, iterator):
    async for height, item in iterator:
        yield (height, random.random(), (item_type, item))


class NodesStatus:
    def __init__(self, initial_height=0):
        self.nodes = {}
        self.resource_nodes = {}
        self.address_nodes = {}
        self.address_staking = {}
        self.balances = {}
        self.platform_balances = {}
        self.last_checked_height = initial_height
        self.last_balance_height = initial_height
        self.last_message_height = initial_height
        self.last_score_height = initial_height
        self.last_eth_balance_height = initial_height
        self.last_others_balance_height = initial_height

    async def update_node_stats(self, node_hash):
        node_info = self.nodes[node_hash]

        node_info["stakers"] = {
            addr: int(
                self.balances.get(addr, 0) / len(self.address_staking.get(addr, []))
            )
            for addr in node_info["stakers"].keys()
        }
        node_info["total_staked"] = sum(node_info["stakers"].values())
        if node_info["total_staked"] >= (ACTIVATION_AMT - (1 * DECIMALS)):
            node_info["status"] = "active"
        else:
            node_info["status"] = "waiting"

    async def remove_node(self, node_hash):
        node = self.nodes[node_hash]
        nodes_to_update = set()
        for staker in node["stakers"].keys():
            self.address_staking[staker].remove(node_hash)
            if len(self.address_staking[staker]) == 0:
                del self.address_staking[staker]
            else:
                nodes_to_update.update(self.address_staking[staker])
        for rnode_hash in node["resource_nodes"]:
            # unlink resource nodes from this parent
            self.resource_nodes[rnode_hash]["parent"] = None
            self.resource_nodes[rnode_hash]["status"] = "waiting"
        self.address_nodes.pop(node["owner"])
        del self.nodes[node_hash]
        [await self.update_node_stats(nhash) for nhash in nodes_to_update]

    async def remove_resource_node(self, node_hash):
        node = self.resource_nodes[node_hash]
        if node["parent"] is not None:
            # unlink the node from the parent
            self.nodes[node["parent"]]["resource_nodes"].remove(node_hash)
            await self.update_node_stats(node["parent"])
        del self.resource_nodes[node_hash]

    async def remove_stake(self, staker, node_hash=None):
        """Removes a staker's stake. If a node_hash isn't given, remove all."""
        # Let's copy it so we can iterate after modification
        node_hashes = self.address_staking[staker].copy()

        if node_hash is not None:
            # Remove specific stake so the total is ok (used in
            # update_node_stats)
            self.address_staking[staker].remove(node_hash)

        for nhash in node_hashes:
            if node_hash is None or nhash == node_hash:
                # if we should remove that stake
                node = self.nodes[nhash]
                if staker in node["stakers"]:
                    node["stakers"].pop(staker)
            await self.update_node_stats(nhash)

        if node_hash is None or len(self.address_staking[staker]) == 0:
            # if we have been asked to remove it all or there is no stake left
            self.address_staking.pop(staker)

    async def _recompute_platform_balances(self, addresses):
        for addr in addresses:
            balance = 0
            for platform in self.platform_balances.keys():
                balance += self.platform_balances[platform].get(addr, 0)
            self.balances[addr] = balance

    def _get_hostname_from_multiaddress(self, multiaddress):
        """ Extract the hostname from a multiaddress """
        try:
            maddr = Multiaddr(multiaddress)
            for protocol in maddr.protocols():
                if protocol.name in ['ip4', 'ip6', 'dns', 'dns4', 'dns6']:
                    return maddr.value_for_protocol(protocol.code)
        except Exception as e:
            print(f"Error parsing multiaddress: {e}")
        return None

    async def _prepare_crn_url(self, node, address=None):
        """ Verify that this URL doesn't exist for another resource node, and return the URL to use """
        if address is None:
            address = node["address"]

        node_hostname = urlparse(address).hostname
        for crn in self.resource_nodes.values():
            # let's extract the hostname of the address
            if crn['hash'] != node['hash']:
                crn_hostname = urlparse(crn["address"]).hostname
                if node_hostname == crn_hostname:
                    return ''

        return address

    async def _prepare_ccn_multiaddress(self, node, multiaddress=None):
        """ Verify that this multiaddress doesn't exist for another core channel node, and return the multiaddress to use """
        if multiaddress is None:
            multiaddress = node["multiaddress"]

        node_hostname = self._get_hostname_from_multiaddress(multiaddress)
        if node_hostname is None:
            return ''

        for ccn in self.nodes.values():
            if ccn['hash'] != node['hash']:
                other_node_hostname = self._get_hostname_from_multiaddress(ccn["multiaddress"])
                if node_hostname == other_node_hostname:
                    return ''

        return multiaddress

    async def process(self, iterators):
        async for height, rnd, (evt_type, content) in merge(*iterators):
            changed = True
            if evt_type == "balance-update":
                balances, platform, changed_addresses = content
                self.platform_balances[platform] = {
                    addr: bal for addr, bal in balances.items()
                }
                await self._recompute_platform_balances(changed_addresses)

                for addr in changed_addresses:
                    if addr in self.address_nodes:
                        if self.balances.get(addr, 0) < NODE_AMT:
                            print(
                                f"{addr}: should delete that node "
                                f"({self.balances.get(addr, 0)})."
                            )

                            await self.remove_node(self.address_nodes[addr])

                    elif addr in self.address_staking:
                        if self.balances.get(addr, 0) < STAKING_AMT:
                            print(
                                f"{addr}: should kill its stake "
                                f"({self.balances.get(addr, 0)})."
                            )
                            await self.remove_stake(addr)
                        else:
                            for nhash in self.address_staking[addr]:
                                await self.update_node_stats(nhash)

                    else:
                        changed = False

                if height > self.last_balance_height:
                    self.last_balance_height = height

                if platform == "ETH" and height > self.last_eth_balance_height:
                    self.last_eth_balance_height = height
                elif platform != "ETH" and height > self.last_others_balance_height:
                    self.last_others_balance_height = height

            elif evt_type == "score-update":
                message_content = content["content"]
                post_type = message_content["type"]
                post_content = message_content["content"]
                address = message_content["address"]

                print(
                    height,
                    post_type,
                    content["sender"]
                )

                if post_type == settings.scores_post_type:
                    for ccn_score in post_content["scores"]["ccn"]:
                        node_id = ccn_score["node_id"]
                        score = ccn_score["total_score"]
                        performance = ccn_score.get("performance", 0)
                        decentralization = ccn_score["decentralization"]
                        if node_id in self.nodes:
                            node = self.nodes[node_id]

                            if self.last_score_height > height - 10:
                                # we got a new score for the same height, only keep the best one
                                if score > node["score"]:
                                    node["score"] = score
                                if performance > node["performance"]:
                                    node["performance"] = performance
                            else:
                                node["score"] = score
                                node["performance"] = performance

                            node["decentralization"] = decentralization
                            node["score_updated"] = True

                            if score < 0.01:
                                if node["inactive_since"] is None:
                                    # we should update the inactive_since only if it's null
                                    node["inactive_since"] = height
                            else:
                                node["inactive_since"] = None

                    for crn_score in post_content["scores"]["crn"]:
                        node_id = crn_score["node_id"]
                        score = crn_score["total_score"]
                        performance = crn_score.get("performance", 0)
                        decentralization = crn_score["decentralization"]
                        if node_id in self.resource_nodes:
                            node = self.resource_nodes[node_id]

                            if self.last_score_height > height - 10:
                                # we got a new score for the same height, only keep the best one
                                if score > node["score"]:
                                    node["score"] = score
                                if performance > node["performance"]:
                                    node["performance"] = performance
                            else:
                                node["score"] = score
                                node["performance"] = performance

                            node["decentralization"] = decentralization
                            node["score_updated"] = True

                            if score < 0.01:
                                if node["inactive_since"] is None:
                                    # we should update the inactive_since only if it's null
                                    node["inactive_since"] = height

                                if (height > settings.crn_inactivity_cutoff_height and
                                    ((height - node["inactive_since"]) > (settings.crn_inactivity_threshold_days * settings.ethereum_blocks_per_day))
                                    and node["parent"] is None):
                                        # we should remove the node
                                    await self.remove_resource_node(node_id)
                            else:
                                node["inactive_since"] = None

                    if height > self.last_score_height:
                        self.last_score_height = height

            elif evt_type == "staking-update":
                message_content = content["content"]
                post_type = message_content["type"]
                post_content = message_content["content"]
                post_action = post_content["action"]
                address = message_content["address"]

                print(
                    height,
                    post_type,
                    content["content"]["address"],
                    content["content"]["content"],
                )

                existing_node = self.address_nodes.get(address, None)
                existing_staking = self.address_staking.get(address, list())
                ref = message_content.get("ref", None)

                if post_type == settings.node_post_type:
                    # Ok it's a wannabe node.
                    # Ignore creation if there is a node already or imbalance
                    if content["item_hash"] == "071bf2d8ea1bb890863f1215a239d1ca5e24fdbfc4a106bc1982600e590f028d":
                        print("### Ok found that NODEEEEE!!!!")
                        print(address in self.address_nodes)
                        print(self.balances.get(address, 0))

                    if (
                        post_action == "create-node"
                        and address not in self.address_nodes
                        and self.balances.get(address, 0) >= NODE_AMT
                    ):
                        details = post_content.get("details", {})
                        new_node = {
                            "hash": content["item_hash"],
                            "owner": address,
                            "reward": details.get("reward", address),
                            "locked": bool(details.get("locked", False)),
                            "stakers": {},
                            "total_staked": 0,
                            "status": "waiting",
                            "time": content["time"],
                            "authorized": [],
                            "resource_nodes": [],
                            "score": 0,
                            "decentralization": 0,
                            "performance": 0,
                            "inactive_since": None
                        }

                        for field in EDITABLE_FIELDS:
                            if field in ["reward", "locked", "authorized"]:
                                # special case already handled
                                continue

                            new_node[field] = details.get(field, "")

                            if field == "multiaddress":
                                # we need to check that the Multiaddress is valid
                                new_node["multiaddress"] = await self._prepare_ccn_multiaddress(new_node)

                        if height < settings.bonus_start:
                            new_node["has_bonus"] = True
                        else:
                            new_node["has_bonus"] = False

                        self.address_nodes[address] = content["item_hash"]
                        self.nodes[content["item_hash"]] = new_node
                        if address in self.address_staking:
                            # remove any existing stake
                            await self.remove_stake(address)

                    elif (
                        post_action == "create-resource-node"
                        and "type" in post_content.get("details", {})
                    ):
                        details = post_content.get("details", {})
                        new_node = {
                            "hash": content["item_hash"],
                            "type": details["type"],
                            "owner": address,
                            "manager": details.get("manager", address),
                            "reward": details.get("reward", address),
                            "locked": bool(details.get("locked", False)),
                            "status": "waiting",
                            "authorized": [],
                            "parent": None,  # parent core node
                            "time": content["time"],
                            "score": 0,
                            "decentralization": 0,
                            "performance": 0,
                            "inactive_since": None
                        }

                        for field in EDITABLE_FIELDS:
                            if field in ["reward"]:
                                # special case already handled
                                continue

                            new_node[field] = details.get(field, "")

                            if field == "address":
                                # we need to check that the URL is valid
                                new_node[field] = await self._prepare_crn_url(new_node)

                        self.resource_nodes[content["item_hash"]] = new_node

                    # resource node to core channel node link
                    elif (
                        post_action == "link"
                        # address should have a ccn
                        and address in self.address_nodes
                        # and it should not be over limit
                        and len(self.nodes[existing_node]["resource_nodes"])
                        < MAX_LINKED
                        and ref is not None
                        # resource node should exist
                        and ref in self.resource_nodes
                        # it shouldn't be linked already
                        and ref not in self.nodes[existing_node]["resource_nodes"]
                        # the target shouldn't have a parent
                        and self.resource_nodes[ref]["parent"] is None
                        # nor be locked
                        and not self.resource_nodes[ref]["locked"]
                    ):
                        node = self.nodes[existing_node]
                        resource_node = self.resource_nodes[ref]
                        node["resource_nodes"].append(ref)
                        resource_node["parent"] = existing_node
                        resource_node["status"] = "linked"
                        await self.update_node_stats(existing_node)

                    elif (
                        post_action == "unlink"
                        and ref is not None
                        and ref in self.resource_nodes
                        and self.resource_nodes[ref]["parent"] is not None
                        # the ccn owner can unlink
                        and (
                            (
                                address in self.address_nodes
                                and (
                                    self.resource_nodes[ref]["parent"] == existing_node
                                )
                                # so does the crn owner
                            )
                            or self.resource_nodes[ref]["owner"] == address
                        )
                    ):
                        resource_node = self.resource_nodes[ref]
                        node = self.nodes[resource_node["parent"]]
                        node["resource_nodes"].remove(ref)
                        resource_node["parent"] = None
                        resource_node["status"] = "waiting"
                        await self.update_node_stats(node["hash"])

                    elif (
                        post_action == "drop-node"
                        and address in self.address_nodes
                        and ref is not None
                        and ref in self.nodes
                        and ref == existing_node
                    ):
                        await self.remove_node(ref)

                    elif (
                        post_action == "drop-node"
                        and ref is not None
                        and ref in self.resource_nodes
                        and self.resource_nodes[ref]["owner"] == address
                    ):
                        await self.remove_resource_node(ref)

                    elif (
                        post_action == "stake"
                        and self.balances.get(address, 0) >= STAKING_AMT
                        and ref is not None
                        and ref in self.nodes
                        and address not in self.address_nodes
                        and (
                            (not self.nodes[ref]["locked"])
                            or (
                                self.nodes[ref]["locked"]
                                and address in self.nodes[ref]["authorized"]
                            )
                        )
                    ):
                        if address in self.address_staking:
                            # remove any existing stake
                            await self.remove_stake(address)
                        self.nodes[ref]["stakers"][address] = self.balances[address]
                        self.address_staking[address] = [
                            ref,
                        ]
                        await self.update_node_stats(ref)

                    elif (
                        post_action == "stake-split"
                        and self.balances.get(address, 0) >= STAKING_AMT
                        and ref is not None
                        and ref in self.nodes
                        and address not in self.address_nodes
                        and ref not in existing_staking
                        and (
                            (not self.nodes[ref]["locked"])
                            or (
                                self.nodes[ref]["locked"]
                                and address in self.nodes[ref]["authorized"]
                            )
                        )
                    ):
                        if address not in self.address_staking:
                            self.address_staking[address] = list()

                        self.address_staking[address].append(ref)
                        self.nodes[ref]["stakers"][address] = int(
                            self.balances[address] / len(self.address_staking[address])
                        )

                        for node_ref in self.address_staking[address]:
                            await self.update_node_stats(node_ref)

                    elif (
                        post_action == "unstake"
                        and address in self.address_staking
                        and ref is not None
                        and ref in existing_staking
                    ):
                        await self.remove_stake(address, ref)

                    else:
                        print("This message wasn't registered (invalid)")
                        changed = False

                elif (
                    post_type == "amend"
                    and ref is not None
                    and ref in self.nodes
                    and (
                        (
                            self.nodes[ref]["owner"] == address
                            and address in self.address_nodes
                        )
                        or self.nodes[ref]["manager"] == address
                    )
                ):
                    node = self.nodes[ref]
                    details = post_content.get("details", {})
                    for field in EDITABLE_FIELDS:
                        if field in ["reward", "manager"]:
                            node[field] = details.get(field, node.get(field, address))
                        elif field == "authorized":
                            node[field] = details.get(field, node.get(field, []))
                        elif field == "locked":
                            node[field] = bool(
                                details.get(field, node.get(field, False))
                            )
                        elif field == "multiaddress":
                            # we need to check that the Multiaddress is valid
                            node[field] = await self._prepare_ccn_multiaddress(node, multiaddress=details.get(field, node.get(field, "")))
                        else:
                            node[field] = details.get(field, node.get(field, ""))

                elif (
                    post_type == "amend"
                    and ref is not None
                    and ref in self.resource_nodes
                    and (
                        self.resource_nodes[ref]["owner"] == address
                        or self.resource_nodes[ref]["manager"] == address
                    )
                ):
                    node = self.resource_nodes[ref]
                    details = post_content.get("details", {})
                    for field in EDITABLE_FIELDS:
                        if field in ["reward", "manager"]:
                            node[field] = details.get(field, node.get(field, address))
                        elif field == "authorized":
                            node[field] = details.get(field, node.get(field, []))
                        elif field == "locked":
                            node[field] = bool(
                                details.get(field, node.get(field, False))
                            )
                        elif field == "address":
                            # we need to check that the URL is valid
                            node[field] = await self._prepare_crn_url(node, address=details.get(field, node.get(field, "")))
                        else:
                            node[field] = details.get(field, node.get(field, ""))

                else:
                    print("This message wasn't registered (invalid)")
                    changed = False

                if height > self.last_message_height:
                    self.last_message_height = height

            if changed:
                yield (height, self.nodes, self.resource_nodes)

            if height > self.last_checked_height:
                self.last_checked_height = height

        # yield(self.last_checked_height, self.nodes)


async def process(dbs):
    state_machine = NodesStatus()
    account = get_aleph_account()

    # Let's keep the last 100 seen TXs aside so we don't count a transfer twice
    # in case of a reorg
    last_seen_txs = deque([], maxlen=100)

    iterators = [
        prepare_items(
            "balance-update",
            process_contract_history(
                settings.ethereum_token_contract,
                settings.ethereum_min_height,
                last_seen=last_seen_txs,
                db=dbs["erc20"],
                fetch_from_db=True
            ),
        ),
        prepare_items(
            "balance-update",
            process_balances_history(
                settings.ethereum_min_height, request_count=500,
                db=dbs["balances"],),
        ),
        prepare_items(
            "staking-update",
            process_message_history(
                [settings.filter_tag],
                [settings.node_post_type, "amend"],
                settings.aleph_api_server,
                request_count=1000,
                db=dbs["messages"],
            ),
        ),
        prepare_items(
            "score-update",
            process_message_history(
                [settings.filter_tag],
                [settings.scores_post_type],
                message_type="POST",
                addresses=settings.scores_senders,
                api_server=settings.aleph_api_server,
                request_count=100,
                db=dbs["scores"],
            ),
        ),
    ]
    nodes = None
    async for height, nodes, resource_nodes in state_machine.process(iterators):
        pass

    print("should set status")
    await set_status(account, nodes, resource_nodes)

    i = 0
    while True:
        i += 1
        iterators = [
            prepare_items(
                "staking-update",
                process_message_history(
                    [settings.filter_tag],
                    [settings.node_post_type, "amend"],
                    settings.aleph_api_server,
                    min_height=state_machine.last_message_height + 1,
                    request_count=1000,
                    crawl_history=False,
                    request_sort="-1",
                    db=dbs["messages"],
                ),
            )
        ]
        if not i % 10:
            iterators.append(
                prepare_items(
                    "balance-update",
                    process_contract_history(
                        settings.ethereum_token_contract,
                        state_machine.last_eth_balance_height + 1,
                        balances={
                            addr: bal
                            for addr, bal in state_machine.platform_balances.get(
                                "ETH", dict()
                            ).items()
                        },
                        last_seen=last_seen_txs,
                        db=dbs["erc20"],
                        fetch_from_db=False
                    ),
                )
            )
        if not i % 60:
            iterators.append(
                prepare_items(
                    "balance-update",
                    process_balances_history(
                        state_machine.last_others_balance_height + 1,
                        crawl_history=False,
                        request_count=100,
                        # TODO: pass platform_balances here
                        request_sort="-1",
                        db=dbs["balances"],
                    ),
                )
            )
        if not i % 3600:
            iterators.append(
                prepare_items(
                    "score-update",
                    process_message_history(
                        [settings.filter_tag],
                        [settings.scores_post_type],
                        message_type="POST",
                        addresses=settings.scores_senders,
                        api_server=settings.aleph_api_server,
                        min_height=state_machine.last_score_height + 1,
                        request_count=50,
                        crawl_history=False,
                        request_sort="-1",
                        db=dbs["scores"],
                    ),
                )
            )

        nodes = None
        async for height, nodes, resource_nodes in state_machine.process(iterators):
            pass

        if nodes is not None:
            await set_status(account, nodes, resource_nodes)
            print("should set status")

        await asyncio.sleep(1)
