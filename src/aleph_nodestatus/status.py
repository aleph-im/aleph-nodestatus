from collections import deque

from aleph_nodestatus.monitored import process_balances_history
from .erc20 import process_contract_history, DECIMALS
from .messages import process_message_history, get_aleph_account, set_status
from .settings import settings
from .utils import merge
import asyncio
import random


NODE_AMT = settings.node_threshold * DECIMALS
STAKING_AMT = settings.staking_threshold * DECIMALS
ACTIVATION_AMT = settings.node_activation * DECIMALS
MAX_LINKED = settings.node_max_linked

EDITABLE_FIELDS = [
    'name',
    'multiaddress',
    'address',
    'picture',
    'banner',
    'description',
    'reward',
    'manager',
    'authorized',
    'locked',
    'registration_url'
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
        self.last_eth_balance_height = initial_height
        self.last_others_balance_height = initial_height

    async def update_node_stats(self, node_hash):
        node_info = self.nodes[node_hash]
                  
        node_info['stakers'] = {addr: int(
                                        self.balances.get(addr, 0) /
                                        len(self.address_staking.get(addr, [])
                                        )) 
                                for addr in node_info['stakers'].keys()}
        node_info['total_staked'] = sum(node_info['stakers'].values())
        if node_info['total_staked'] >= (ACTIVATION_AMT-(1*DECIMALS)):
            node_info['status'] = 'active'
        else:
            node_info['status'] = 'waiting'

    async def remove_node(self, node_hash):
        node = self.nodes[node_hash]
        nodes_to_update = set()
        for staker in node['stakers'].keys():
            self.address_staking[staker].remove(node_hash)
            if len(self.address_staking[staker]) == 0:
                del self.address_staking[staker]
            else:
                nodes_to_update.update(self.address_staking[staker])
        for rnode_hash in node['resource_nodes']:
            # unlink resource nodes from this parent
            self.resource_nodes[rnode_hash]['parent'] = None
            self.resource_nodes[rnode_hash]['status'] = "waiting"
        self.address_nodes.pop(node['owner'])
        del self.nodes[node_hash]
        [await self.update_node_stats(nhash) for nhash in nodes_to_update]
        
    async def remove_resource_node(self, node_hash):
        node = self.resource_nodes[node_hash]
        if node['parent'] is not None:
            # unlink the node from the parent
            self.nodes[node['parent']]['resource_nodes'].remove(node_hash)
            await self.update_node_stats(node['parent'])
        del self.resource_nodes[node_hash]

    async def remove_stake(self, staker, node_hash=None):
        """ Removes a staker's stake. If a node_hash isn't given, remove all.
        """
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
                if staker in node['stakers']:
                    node['stakers'].pop(staker)
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

    async def process(self, iterators):
        async for height, rnd, (evt_type, content) in merge(*iterators):
            changed = True
            if evt_type == 'balance-update':
                balances, platform, changed_addresses = content
                self.platform_balances[platform] = {
                    addr: bal
                    for addr, bal in balances.items()}
                await self._recompute_platform_balances(changed_addresses)
                
                for addr in changed_addresses:
                    if addr in self.address_nodes:
                        if self.balances.get(addr, 0) < NODE_AMT:
                            print(f"{addr}: should delete that node "
                                  f"({self.balances.get(addr, 0)}).")

                            await self.remove_node(self.address_nodes[addr])

                    elif addr in self.address_staking:
                        if self.balances.get(addr, 0) < STAKING_AMT:
                            print(f"{addr}: should kill its stake "
                                  f"({self.balances.get(addr, 0)}).")
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

            elif evt_type == 'staking-update':
                message_content = content['content']
                post_type = message_content['type']
                post_content = message_content['content']
                post_action = post_content['action']
                address = message_content['address']

                print(height,
                      post_type,
                      content['content']['address'],
                      content['content']['content'])

                existing_node = self.address_nodes.get(address, None)
                existing_staking = self.address_staking.get(address, list())
                ref = message_content.get('ref', None)

                if post_type == settings.node_post_type:
                    # Ok it's a wannabe node.
                    # Ignore creation if there is a node already or imbalance
                    if (post_action == "create-node"
                            and address not in self.address_nodes
                            and self.balances.get(address, 0) >= NODE_AMT):
                        details = post_content.get('details', {})
                        new_node = {
                            'hash': content['item_hash'],
                            'owner': address,
                            'reward': details.get('reward', address),
                            'locked': bool(details.get('locked', False)),
                            'stakers': {},
                            'total_staked': 0,
                            'status': 'waiting',
                            'time': content['time'],
                            'authorized': [],
                            'resource_nodes': []
                        }
                        
                        for field in EDITABLE_FIELDS:
                            if field in ['reward', 'locked', 'authorized']:
                                # special case already handled
                                continue
                            
                            new_node[field] = details.get(field, '')
                            
                        if height < settings.bonus_start:
                            new_node['has_bonus'] = True
                        else:
                            new_node['has_bonus'] = False
                            
                        self.address_nodes[address] = content['item_hash']
                        self.nodes[content['item_hash']] = new_node
                        if address in self.address_staking:
                            # remove any existing stake
                            await self.remove_stake(address)
                            
                    elif (post_action == "create-resource-node"
                          and 'type' in post_content.get('details', {})):
                        details = post_content.get('details', {})
                        new_node = {
                            'hash': content['item_hash'],
                            'type': details['type'],
                            'owner': address,
                            'manager': details.get('manager', address),
                            'reward': details.get('reward', address),
                            'locked': bool(details.get('locked', False)),
                            'status': 'waiting',
                            'authorized': [],
                            'parent': None, # parent core node
                            'time': content['time']
                        }
                        
                        for field in EDITABLE_FIELDS:
                            if field in ['reward']:
                                # special case already handled
                                continue
                            
                            new_node[field] = details.get(field, '')
                            
                        self.resource_nodes[content['item_hash']] = new_node
                    
                    # resource node to core channel node link
                    elif (post_action == 'link'
                          # address should have a ccn
                          and address in self.address_nodes
                          # and it should not be over limit
                          and len(self.nodes[existing_node]['resource_nodes']) < MAX_LINKED
                          and ref is not None
                          # resource node should exist
                          and ref in self.resource_nodes
                          # it shouldn't be linked already
                          and ref not in self.nodes[existing_node]['resource_nodes']
                          # the target shouldn't have a parent
                          and self.resource_nodes[ref]['parent'] is None
                          # nor be locked
                          and not self.resource_nodes[ref]['locked']):
                        
                        node = self.nodes[existing_node]
                        resource_node = self.resource_nodes[ref]
                        node['resource_nodes'].append(ref)
                        resource_node['parent'] = existing_node
                        resource_node['status'] = "linked"
                        await self.update_node_stats(existing_node)
                        
                    elif (post_action == 'unlink'
                          and ref is not None
                          and ref in self.resource_nodes
                          and self.resource_nodes[ref]['parent'] is not None
                          # the ccn owner can unlink
                          and ((address in self.address_nodes
                                and (self.resource_nodes[ref]['parent'] ==
                                     existing_node)
                                # so does the crn owner
                                ) or
                               self.resource_nodes[ref]['owner'] == address)):
                        resource_node = self.resource_nodes[ref]
                        node = self.nodes[resource_node['parent']]
                        node['resource_nodes'].remove(ref)
                        resource_node['parent'] = None
                        resource_node['status'] = "waiting"
                        await self.update_node_stats(node['hash'])

                    elif (post_action == "drop-node"
                          and address in self.address_nodes
                          and ref is not None
                          and ref in self.nodes
                          and ref == existing_node):
                        await self.remove_node(ref)

                    elif (post_action == "drop-node"
                          and ref is not None
                          and ref in self.resource_nodes
                          and self.resource_nodes[ref]['owner'] == address):
                        await self.remove_resource_node(ref)

                    elif (post_action == "stake"
                            and self.balances.get(address, 0) >= STAKING_AMT
                            and ref is not None and ref in self.nodes
                            and address not in self.address_nodes
                            and ((not self.nodes[ref]['locked'])
                                 or (self.nodes[ref]['locked']
                                     and address in self.nodes[ref]['authorized']))):
                        if address in self.address_staking:
                            # remove any existing stake
                            await self.remove_stake(address)
                        self.nodes[ref]['stakers'][address] =\
                            self.balances[address]
                        self.address_staking[address] = [ref, ]
                        await self.update_node_stats(ref)
                        
                    elif (post_action == "stake-split"
                            and self.balances.get(address, 0) >= STAKING_AMT
                            and ref is not None and ref in self.nodes
                            and address not in self.address_nodes
                            and ref not in existing_staking
                            and ((not self.nodes[ref]['locked'])
                                 or (self.nodes[ref]['locked']
                                     and address in self.nodes[ref]['authorized'])
                                 )):
                        if address not in self.address_staking:
                            self.address_staking[address] = list()
                            
                        self.address_staking[address].append(ref)
                        self.nodes[ref]['stakers'][address] =\
                            int(self.balances[address] /
                                len(self.address_staking[address]))
                            
                        for node_ref in self.address_staking[address]:
                            await self.update_node_stats(node_ref)

                    elif (post_action == "unstake"
                          and address in self.address_staking
                          and ref is not None
                          and ref in existing_staking):
                        
                        await self.remove_stake(address, ref)

                    else:
                        print("This message wasn't registered (invalid)")
                        changed = False
                        
                elif (post_type == 'amend'
                      and ref is not None
                      and ref in self.nodes
                      and ((self.nodes[ref]['owner'] == address
                            and address in self.address_nodes)
                           or self.nodes[ref]['manager'] == address)):
                    node = self.nodes[ref]
                    details = post_content.get('details', {})
                    for field in EDITABLE_FIELDS:
                        if field in ['reward', 'manager']:
                            node[field] = details.get(field,
                                                      node.get(field, address))
                        elif field == 'authorized':
                            node[field] = details.get(field,
                                                      node.get(field, []))
                        elif field == 'locked':
                            node[field] = bool(details.get(field,
                                                        node.get(field, False)))
                        else:
                            node[field] = details.get(field,
                                                      node.get(field, ''))
                        
                elif (post_type == 'amend'
                      and ref is not None
                      and ref in self.resource_nodes
                      and (self.resource_nodes[ref]['owner'] == address
                           or self.resource_nodes[ref]['manager'] == address)):
                    node = self.resource_nodes[ref]
                    details = post_content.get('details', {})
                    for field in EDITABLE_FIELDS:
                        if field in ['reward', 'manager']:
                            node[field] = details.get(field,
                                                      node.get(field, address))
                        elif field == 'authorized':
                            node[field] = details.get(field,
                                                      node.get(field, []))
                        elif field == 'locked':
                            node[field] = bool(details.get(field,
                                                        node.get(field, False)))
                        else:
                            node[field] = details.get(field,
                                                      node.get(field, ''))
                    
                else:
                    print("This message wasn't registered (invalid)")
                    changed = False
                        
                if height > self.last_message_height:
                    self.last_message_height = height

            if changed:
                yield(height, self.nodes, self.resource_nodes)

            if height > self.last_checked_height:
                self.last_checked_height = height
            
        # yield(self.last_checked_height, self.nodes)


async def process():
    state_machine = NodesStatus()
    account = get_aleph_account()
    
    # Let's keep the last 100 seen TXs aside so we don't count a transfer twice
    # in case of a reorg
    last_seen_txs = deque([], maxlen=100)

    iterators = [
        prepare_items('balance-update', process_contract_history(
            settings.ethereum_token_contract, settings.ethereum_min_height,
            last_seen=last_seen_txs)),
        prepare_items('balance-update', process_balances_history(
            settings.ethereum_min_height)),
        prepare_items('staking-update', process_message_history(
            [settings.filter_tag],
            [settings.node_post_type, 'amend'],
            settings.aleph_api_server)
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
            prepare_items('staking-update',
                          process_message_history(
                              [settings.filter_tag],
                              [settings.node_post_type, 'amend'],
                              settings.aleph_api_server,
                              min_height=state_machine.last_message_height+1,
                              request_count=1000,
                              crawl_history=False,
                              request_sort='-1'))
        ]
        if not i % 10:
            iterators.append(
                prepare_items('balance-update',
                              process_contract_history(
                                  settings.ethereum_token_contract,
                                  state_machine.last_eth_balance_height+1,
                                  balances={addr: bal
                                            for addr, bal
                                            in state_machine.platform_balances.get('ETH', dict()).items()},
                                  last_seen=last_seen_txs)
                              ))
        if not i % 60:
            iterators.append(
                prepare_items('balance-update',
                              process_balances_history(
                                  state_machine.last_others_balance_height+1,
                                  crawl_history=False,
                                  request_count=100,
                                  # TODO: pass platform_balances here
                                  request_sort='-1')
                              ))
            
        nodes = None
        async for height, nodes, resource_nodes in state_machine.process(iterators):
            pass

        if nodes is not None:
            await set_status(account, nodes, resource_nodes)
            print("should set status")

        await asyncio.sleep(1)
        