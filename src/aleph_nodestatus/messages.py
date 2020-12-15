import aiohttp
from .settings import settings


async def process_message_history(tags, message_types, api_server):
    async with aiohttp.ClientSession() as session:
        async with session.get(f'{api_server}/api/v0/messages.json', params={
                'msgType': 'POST',
                'tags': ','.join(tags),
                'contentTypes': ','.join(message_types),
                'pagination': 100000,
                'sort_order': '1'
                }) as resp:
            items = await resp.json()
            print(items)
            for message in items['messages']:
                earliest = None
                for conf in message.get('confirmations', []):
                    if conf['chain'] == 'ETH':
                        if earliest is None or conf['height'] < earliest:
                            earliest = conf['height']
                if earliest is not None:
                    yield earliest, message
