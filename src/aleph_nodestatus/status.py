from .erc20 import process_contract_history, DECIMALS
from .messages import process_message_history, get_aleph_account, set_status
from .settings import settings
from .utils import merge
import asyncio


NODE_AMT = settings.node_threshold
STAKING_AMT = settings.staking_threshold
ACTIVATION_AMT = settings.node_activation


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
        node_info['stakers'] = {addr: self.balances.get(addr, 0)
                                for addr in node_info['stakers'].keys()}
        node_info['total_staked'] = sum(node_info['stakers'].values())
        if node_info['total_staked'] >= ACTIVATION_AMT:
            node_info['status'] = 'active'
        else:
            node_info['status'] = 'waiting'

    async def remove_node(self, node_hash):
        node = self.nodes[node_hash]
        for staker in node['stakers'].keys():
            del self.address_staking[staker]
        self.address_nodes.pop(node['owner'])
        del self.nodes[node_hash]

    async def remove_stake(self, staker):
        node_hash = self.address_staking[staker]
        node = self.nodes[node_hash]
        if staker in node['stakers']:
            node['stakers'].pop(staker)
        await self.update_node_stats(node_hash)
        self.address_staking.pop(staker)

    async def process(self, iterators):
        async for height, (evt_type, content) in merge(*iterators):
            changed = True
            if evt_type == 'balance-update':
                balances, changed_addresses = content
                self.balances = {addr: bal/DECIMALS
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
                            await self.update_node_stats(
                                self.address_staking[addr])

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
                existing_staking = self.address_staking.get(address, None)
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
                            'reward': details.get('reward_address', address),
                            'name': details.get('name', ''),
                            'multiaddress': details.get('multiaddress', ''),
                            'stakers': {},
                            'total_staked': 0,
                            'status': 'waiting',
                            'time': content['time']
                        }
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
                            and address not in self.address_nodes):
                        if address in self.address_staking:
                            # remove any existing stake
                            await self.remove_stake(address)
                        self.nodes[ref]['stakers'][address] =\
                            self.balances[address]
                        self.address_staking[address] = ref
                        await self.update_node_stats(ref)

                    elif (post_action == "unstake"
                          and address in self.address_staking
                          and ref is not None
                          and existing_staking == ref):
                        await self.remove_stake(address)

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

    iterators = [
        prepare_items('balance-update', process_contract_history(
            settings.ethereum_token_contract, settings.ethereum_min_height)),
        prepare_items('staking-update', process_message_history(
            [settings.filter_tag],
            [settings.node_post_type],
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
                              [settings.node_post_type],
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
                                  balances={addr: bal*DECIMALS
                                            for addr, bal
                                            in state_machine.balances.items()})
            ))
        nodes = None
        async for height, nodes in state_machine.process(iterators):
            pass

        if nodes is not None:
            await set_status(account, nodes)
            print("should set status")

        await asyncio.sleep(1)
        