from collections import deque
from .erc20 import process_contract_history, DECIMALS
from .messages import process_message_history, get_aleph_account, set_status
from .settings import settings
from .utils import merge
import asyncio


NODE_AMT = settings.node_threshold * DECIMALS
STAKING_AMT = settings.staking_threshold * DECIMALS
ACTIVATION_AMT = settings.node_activation * DECIMALS

EDITABLE_FIELDS = [
    'name',
    'multiaddress',
    'picture',
    'banner',
    'description',
    'reward',
    'locked'
]


async def prepare_items(item_type, iterator):
    async for height, item in iterator:
        yield (height, (item_type, item))


class NodesStatus:
    def __init__(self, initial_height=0):
        self.nodes = {}
        self.address_nodes = {}
        self.address_staking = {}
        self.balances = {}
        self.last_checked_height = initial_height
        self.last_balance_height = initial_height
        self.last_message_height = initial_height

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
        self.address_nodes.pop(node['owner'])
        del self.nodes[node_hash]
        [await self.update_node_stats(nhash) for nhash in nodes_to_update]

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

    async def process(self, iterators):
        async for height, (evt_type, content) in merge(*iterators):
            changed = True
            if evt_type == 'balance-update':
                balances, changed_addresses = content
                self.balances = {addr: bal
                                 for addr, bal in balances.items()}
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
                            'time': content['time']
                        }
                        
                        for field in EDITABLE_FIELDS:
                            if field in ['reward', 'locked']:
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

                    elif (post_action == "drop-node"
                          and address in self.address_nodes):
                        if ref is not None and ref in self.nodes:
                            if ref == existing_node:
                                await self.remove_node(ref)

                    elif (post_action == "stake"
                            and self.balances.get(address, 0) >= STAKING_AMT
                            and ref is not None and ref in self.nodes
                            and address not in self.address_nodes
                            and not self.nodes[ref]['locked']):
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
                            and not self.nodes[ref]['locked']):
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
                      and address in self.address_nodes
                      and ref is not None
                      and ref in self.nodes
                      and ref == existing_node):
                    node = self.nodes[ref]
                    details = post_content.get('details', {})
                    for field in EDITABLE_FIELDS:
                        if field == 'reward':
                            node[field] = details.get(field,
                                                      node.get(field, address))
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
                yield(height, self.nodes)

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
        prepare_items('staking-update', process_message_history(
            [settings.filter_tag],
            [settings.node_post_type, 'amend'],
            settings.aleph_api_server))
    ]
    nodes = None
    async for height, nodes in state_machine.process(iterators):
        pass
    
    print("should set status")
    await set_status(account, nodes)

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
                              request_sort='-1'))
        ]
        if not i % 10:
            iterators.append(
                prepare_items('balance-update',
                              process_contract_history(
                                  settings.ethereum_token_contract,
                                  state_machine.last_balance_height+1,
                                  balances={addr: bal
                                            for addr, bal
                                            in state_machine.balances.items()},
                                  last_seen=last_seen_txs)
            ))
        nodes = None
        async for height, nodes in state_machine.process(iterators):
            pass

        if nodes is not None:
            await set_status(account, nodes)
            print("should set status")

        await asyncio.sleep(1)
        