import asyncio

from aleph_nodestatus.status import NodesStatus


ADDRESS = "0x84Dfa9A9B8f7319b150137BdfDd03cc6617b2165"
REF = "ef95cfc6e770644c8517fcf9889a1e49d5b275788f1a9b23d09d74a4b1f62ced"


def _resource_node():
    return {
        "hash": REF,
        "owner": ADDRESS,
        "manager": ADDRESS,
        "reward": ADDRESS,
        "locked": False,
        "authorized": [],
        "name": "old-name",
        "address": "https://old.example",
        "multiaddress": "",
        "type": "compute",
        "status": "waiting",
        "parent": None,
        "score": 0,
        "decentralization": 0,
        "performance": 0,
        "inactive_since": None,
        "time": 1,
        "picture": "",
        "banner": "",
        "description": "",
        "registration_url": "",
        "terms_and_conditions": "",
        "stream_reward": "",
    }


async def _events():
    yield (
        123,
        (0.0, ""),
        (
            "staking-update",
            {
                "item_hash": "amend-without-action",
                "time": 1795014998.99,
                "content": {
                    "type": "amend",
                    "address": ADDRESS,
                    "ref": REF,
                    "content": {
                        "details": {"name": "new-name"},
                        "tags": ["mainnet"],
                    },
                },
            },
        ),
    )


def test_amend_without_action_updates_resource_node():
    state = NodesStatus()
    state.resource_nodes[REF] = _resource_node()

    async def run():
        async for _ in state.process([_events()]):
            pass

    asyncio.run(run())

    assert state.resource_nodes[REF]["name"] == "new-name"
