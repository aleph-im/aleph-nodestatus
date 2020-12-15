from .erc20 import process_contract_history, DECIMALS
from .messages import process_message_history
from .settings import settings
from .utils import merge


NODE_AMT = settings.node_threshold * DECIMALS
STAKING_AMT = settings.staking_threshold * DECIMALS


async def prepare_items(item_type, iterator):
    async for height, item in iterator:
        yield (height, (item_type, item))


async def update_node_stats(balances, node_info):
    node_info['stakers'] = {addr: balances[addr]
                            for addr in node_info['stakers'].keys()}
    node_info['total_staked'] = sum(node_info['stakers'].values())
    return node_info


async def remove_node(node_hash, address_nodes, address_staking, nodes):
    node = nodes[node_hash]
    for staker in node['stakers'].keys():
        del address_staking[staker]
    address_nodes.pop(node['owner'])
    del nodes[node_hash]


async def remove_stake(staker, address_staking, nodes, balances):
    node_hash = address_staking[staker]
    node = nodes[node_hash]
    if staker in node['stakers']:
        node['stakers'].pop(staker)
    await update_node_stats(balances, node)
    address_staking.pop(staker)


async def process():
    iterators = [
        prepare_items('balance-update', process_contract_history(
            settings.ethereum_token_contract, settings.ethereum_min_height)),
        prepare_items('staking-update', process_message_history(
            [settings.filter_tag],
            [settings.node_post_type,
             settings.staker_post_type],
            settings.aleph_api_server))
    ]

    # Initialize the state machine
    nodes = {}
    address_nodes = {}
    address_staking = {}
    balances = {}

    async for height, (evt_type, content) in merge(*iterators):
        changed = True
        if evt_type == 'balance-update':
            balances, changed_addresses = content
            for addr in changed_addresses:
                if addr in address_nodes:
                    if balances[addr] < NODE_AMT:
                        print(f"{addr}: should delete that node "
                              f"({balances[addr]/DECIMALS}).")

                        await remove_node(address_nodes[addr], address_nodes,
                                          address_staking, nodes)

                elif addr in address_staking:
                    if balances[addr] < STAKING_AMT:
                        print(f"{addr}: should kill its stake "
                              f"({balances[addr]/DECIMALS}).")
                        await remove_stake(addr, address_staking,
                                           nodes, balances)
                    else:
                        await update_node_stats(balances,
                                                nodes[address_staking[addr]])
                        
                else:
                    changed = False

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

            existing_node = address_nodes.get(address, None)
            existing_staking = address_staking.get(address, None)
            ref = message_content.get('ref', None)
            
            if post_type == settings.node_post_type:
                # Ok it's a wannabe node.
                # Ignore creation if there is a node already or if imbalance
                if (post_action == "create-node"
                        and address not in address_nodes
                        and balances[address] > NODE_AMT):
                    details = post_content.get('details', {})
                    new_node = {
                        'hash': content['item_hash'],
                        'owner': address,
                        'reward': details.get('reward_address', address),
                        'name': details.get('name', ''),
                        'multiaddress': details.get('multiaddress', ''),
                        'stakers': {},
                        'total_staked': 0,
                        'status': 'waiting'
                    }
                    address_nodes = content['item_hash']
                    nodes[content['item_hash']] = new_node
                    if address in address_staking:
                        # remove any existing stake
                        await remove_stake(address, address_staking,
                                           nodes, balances)
                        
                elif (post_action == "drop-node"
                      and address in address_nodes):
                    if ref is not None and ref in nodes:
                        if ref == existing_node:
                            await remove_node(ref, address_nodes,
                                              address_staking, nodes)

            elif post_type == settings.staker_post_type:
                if (post_action == "stake"
                        and balances[address] > STAKING_AMT
                        and ref is not None and ref in nodes):
                    if address in address_staking:
                        # remove any existing stake
                        await remove_stake(address, address_staking,
                                           nodes, balances)
                    nodes[ref][address] = balances[address]
                    address_staking[address] = ref
                    await update_node_stats(balances, nodes[ref])
                    
                elif (post_action == "unstake"
                      and address in address_staking
                      and ref is not None
                      and existing_staking == ref):
                    await remove_stake(address, address_staking,
                                       nodes, balances)
        if changed:
            print(height, nodes)
    