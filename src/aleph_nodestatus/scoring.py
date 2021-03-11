import asyncio
import dataclasses
import json
import logging
import time
from dataclasses import dataclass
from functools import partial
from typing import Dict, Iterator, NewType, Optional, Coroutine, Union

import aiohttp
import libp2p
from aleph_client.asynchronous import create_post
from aleph_client.chains.ethereum import get_fallback_account
from dataclasses_json import dataclass_json
from libp2p import new_node, BasicHost
from libp2p.crypto.secp256k1 import create_new_key_pair
from libp2p.peer.peerinfo import info_from_p2p_addr
from multiaddr import multiaddr, Multiaddr

logger = logging.getLogger()
logger.setLevel(logging.ERROR)
Url = NewType('Url', str)

MAX_LATENCY = 5.

@dataclass_json
@dataclass
class Node:
    """Node data from the aggregate."""
    hash: str
    owner: str
    reward: str
    stakers: dict
    total_staked: float
    status: str
    time: float
    name: str
    multiaddress: str
    picture: str
    banner: str
    description: str


@dataclass_json
@dataclass
class NodeMetrics:
    """Metrics obtained from a node."""
    p2p_connect_latency: Optional[Union[float, Exception]] = None
    http_index_latency: Optional[Union[float, Exception]] = None
    http_aggregate_latency: Optional[Union[float, Exception]] = None
    http_store_latency: Optional[Union[float, Exception]] = None

    def get_errors(self) -> Iterator[Exception]:
        for attr in dataclasses.fields(self):
            attr: dataclasses.Field
            value = getattr(self, attr.name)
            if isinstance(value, Exception):
                yield value

    def contains_error(self) -> bool:
        return any(self.get_errors())

    def global_score(self) -> float:
        if not self.contains_error():
            total = 1.
            for field in dataclasses.fields(self):
                value: float = getattr(self, field.name) / MAX_LATENCY
                score = max(1 - value or 1, 0)
                total *= score
            return total
        else:
            return 0.

    def __str__(self):
        result = ""
        for value in (
            self.p2p_connect_latency,
            self.http_index_latency,
            self.http_aggregate_latency,
            self.http_store_latency,
            self.global_score()
        ):
            if isinstance(value, Exception):
                result += "error "
            else:
                result += f"{value:.3f} "
        return result


    def to_dict_with_score(self) -> Dict:
        result = {}
        for attr in dataclasses.fields(self):
            value = getattr(self, attr.name)
            if isinstance(value, Exception):
                result[attr.name] = str(value)
            else:
                result[attr.name] = float(value)
        result['global_score'] = self.global_score()
        return result


async def get_aggregate(url: Url):
    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            resp.raise_for_status()
            return await resp.json()


def nodes_from_aggregate(aggregate: Dict) -> Iterator[Node]:
    for node in aggregate['data']['corechannel']['nodes']:
        yield Node.from_dict(node)


async def measure_coroutine_latency(coroutine: Coroutine, timeout: float=MAX_LATENCY
                                    ) -> Optional[float]:
    """Execute a coroutine and return how much time it took to execute."""
    t0 = time.time()
    dt: float
    try:
        await asyncio.wait_for(coroutine, timeout=timeout)
        return time.time() - t0
    except asyncio.TimeoutError:
        raise TimeoutError(f"Cancelled '{coroutine.__name__}' after {timeout} seconds")


async def p2p_connect(host: BasicHost, address: Multiaddr) -> Optional[float]:
    """Connect to a node using libp2p."""
    info = info_from_p2p_addr(address)
    await host.connect(info),


async def http_get_index(address: Multiaddr) -> aiohttp.ClientResponse:
    """Get the index of a node using the HTTP API"""
    ip4 = address.value_for_protocol('ip4')
    url = f"http://{ip4}:4024/"

    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                return resp
            else:
                resp.raise_for_status()


async def http_get_aggregate(address: Multiaddr) -> aiohttp.ClientResponse:
    """Get a copy of the aggregate from a node using the HTTP API"""
    ip4 = address.value_for_protocol('ip4')
    url = f"http://{ip4}:4024/api/v0/aggregates/0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10.json"

    timeout = aiohttp.ClientTimeout(total=60)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(url) as resp:
            if resp.status == 200:
                return resp
            else:
                resp.raise_for_status()


store_message = {
        "_id" : "6009dbdd9b6d38b89d36bb4a",
        "chain" : "ETH",
        "item_hash" : "ddb72fa6751d59546ad3156bb250151d9308be6c957e9ac57d297ec566ae806a",
        "sender" : "0x6696450BA99d0bDcAB713c33F2bf106D746485D2",
        "type" : "STORE",
        "channel" : "MYALEPH",
        "confirmations" : [
                {
                        "chain" : "ETH",
                        "height" : 11528852,
                        "hash" : "0x88870e4d4c16bc36ba356421d7926917b80d0d5d8a06471e6463ea7895161a24"
                }
        ],
        "content" : {
                "address" : "0x6696450BA99d0bDcAB713c33F2bf106D746485D2",
                "item_type" : "ipfs",
                "item_hash" : "QmaQVXbihn3Ln2oSrVdL1oby832pxthXN9j669E9hwugVd",
                "time" : 1608978939.585,
                "engine_info" : {
                        "Hash" : "QmaQVXbihn3Ln2oSrVdL1oby832pxthXN9j669E9hwugVd",
                        "Size" : 72639,
                        "CumulativeSize" : 72653,
                        "Blocks" : 0,
                        "Type" : "file"
                },
                "size" : 72653,
                "content_type" : "file"
        },
        "item_content" : "{\"address\":\"0x6696450BA99d0bDcAB713c33F2bf106D746485D2\",\"item_type\":\"ipfs\",\"item_hash\":\"QmaQVXbihn3Ln2oSrVdL1oby832pxthXN9j669E9hwugVd\",\"time\":1608978939.585}",
        "item_type" : "inline",
        "signature" : "0xa80a8f30d03a2d87f1d1f6be568627ad7009314238305fbfe853e74620e90222738ab81dcc86a8e0d1fdbf65c927d7892a4d10d4513a810edfb1093348d46a5c1b",
        "size" : 158,
        "time" : 1608979428.008
}


async def get_random_message() -> Dict:
    """Get a random message from an Aleph Node"""
    async with aiohttp.ClientSession() as session:
        async with session.get('http://163.172.70.92:4024/api/v0/messages/random.json') as resp:
            print(resp.status)
            resp.raise_for_status()
            result = await resp.json()
            print(result)
    return result


def get_ipfs_cid_from_url(url: str) -> bytes:
    """Compute the IPFS CID of the data downloaded at an url.

    Streams the data to IPFS, hoping to avoid storing the entire file in memory or disk.
    $ curl --output - "https://releases.ubuntu.com/20.04.1/ubuntu-20.04.1-desktop-amd64.iso" | ipfs add --only-hash -Q
    """
    from subprocess import Popen, PIPE
    curl_process = Popen(['curl', '--output', '-', '--silent', url], stdout=PIPE)
    ipfs_process = Popen(['ipfs', 'add', '--only-hash', '-Q'],
                         stdin=curl_process.stdout, stdout=PIPE)
    curl_process.stdout.close()  # enable write error in dd if ssh dies
    out, err = ipfs_process.communicate()

    logger.debug("Downloading URL", url)
    if err:
        raise ValueError(err)
    return out.strip()


def get_sha256_from_url(url: str) -> bytes:
    """Compute the sha256 sum of the data downloaded at an url.

    This could be implemented in Python, but it was easy to fork the IPFS version above
    and may be more performant this way - or not.
    $ curl --output - "https://releases.ubuntu.com/20.04.1/ubuntu-20.04.1-desktop-amd64.iso" | sha256sum
    """
    from subprocess import Popen, PIPE
    curl_process = Popen(['curl', '--output', '-', '--silent', url], stdout=PIPE)
    sha256_process = Popen(['sha256sum'],
                         stdin=curl_process.stdout, stdout=PIPE)
    curl_process.stdout.close()  # enable write error in dd if ssh dies
    out, err = sha256_process.communicate()

    logger.debug("Downloading URL", url)
    if err:
        raise ValueError(err)
    return out.strip().split(b' ')[0]


async def http_get_stored_file(address: Multiaddr, message: Dict) -> bool:
    """Get a file from a node using the HTTP API"""
    ip4 = address.value_for_protocol('ip4')
    item_hash = message['content']['item_hash'].encode()
    url = f"http://{ip4}:4024/api/v0/storage/raw/{item_hash.decode()}"

    if message['content']['item_type'] == 'ipfs':
        loop = asyncio.get_event_loop()
        served_hash = await loop.run_in_executor(None, partial(get_ipfs_cid_from_url, url))
    elif message['content']['item_type'] == 'storage':
        loop = asyncio.get_event_loop()
        served_hash = await loop.run_in_executor(None, partial(get_sha256_from_url, url))
    else:
        raise ValueError

    if item_hash != served_hash:
        raise ValueError(f"Hashes differ: '{item_hash.decode()}' != '{served_hash.decode()}'")


secret = b'#\xb8\xc1\xe99$V\xde>\xb1;\x90FhRW\xbd\xd6@\xfb\x06g\x1a\xd1\x1c\x801\x7f\xa3\xb1y\x9d'
transport_opt = f"/ip4/127.0.0.1/tcp/1234"


async def compute_node_metrics(multiaddress: str, p2p_host, random_message: Dict):
    metrics = NodeMetrics()

    try:
        address = Multiaddr(multiaddress)
    except multiaddr.exceptions.StringParseError as error:
        for field in dataclasses.fields(metrics):
            field: dataclasses.Field
            setattr(metrics, field.name, error)
        return metrics

    for name, coroutine in {
        'p2p_connect_latency': p2p_connect(p2p_host, address),
        'http_index_latency': http_get_index(address),
        'http_aggregate_latency': http_get_aggregate(address),
        'http_store_latency': http_get_stored_file(address, random_message),
    }.items():
        try:
            measurement = await measure_coroutine_latency(coroutine)
            setattr(metrics, name, measurement)
        except (
                libp2p.network.exceptions.SwarmException,
                aiohttp.client_exceptions.ClientResponseError,
                ValueError,
                TimeoutError,
        ) as error:
            setattr(metrics, name, error)
    return metrics


async def main():
    aggregate_url = Url("https://api2.aleph.im/api/v0/aggregates/0xa1B3bb7d2332383D96b7796B908fB7f7F3c2Be10.json")
    aggregate = await get_aggregate(aggregate_url)

    p2p_host = await new_node(
        key_pair=create_new_key_pair(secret), transport_opt=[transport_opt]
    )

    nodes = list(nodes_from_aggregate(aggregate))

    scores: Dict[str, NodeMetrics] = {}

    # random_message = store_message
    random_message = await get_random_message()
    print(f"Scoring using message {random_message['_id']}")

    for node in nodes:
        metrics = await compute_node_metrics(node.multiaddress,
                                             p2p_host,
                                             random_message)

        print(metrics.contains_error())
        scores[node.hash] = metrics.to_dict_with_score()
        print(node.hash, metrics)

    account = get_fallback_account()
    content = {
        'content': {
            'scores': scores,
        },
        'tags': [],
    }
    print(json.dumps(content))
    post = await create_post(
        account,
        content,
        post_type='scoring',
        channel='TEST',
        api_server="https://api2.aleph.im")
    print(post)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

