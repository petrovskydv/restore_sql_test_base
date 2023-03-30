import asyncio
import json
import logging
import os.path
from asyncio import Queue
from pathlib import Path

import aiohttp
from aiohttp import web
from anyio import create_task_group

from services.service import async_do_restore

BASE_DIR = Path(__file__).resolve().parent

logger = logging.getLogger(__name__)


async def handle(request):
    index_path = os.path.join(BASE_DIR, 'index.html')
    with open(index_path, 'r') as f:
        file = f.read()

    return web.Response(body=file, headers={'Content-Type': 'text/html', })


async def send_msg(messages_queue, ws):
    while True:
        try:
            message = await messages_queue.get()
            logger.debug(f'send msg to browser {message}')
            await ws.send_str(message)
        except ConnectionResetError:
            logger.error('ConnectionResetError')


async def websocket_handler(request):
    logger.debug('start websocket')
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    messages_queue = Queue()

    async for msg in ws:
        logger.debug(f'{msg=}')
        if msg.type == aiohttp.WSMsgType.TEXT:
            if msg.data == 'close':
                await ws.close()
            else:
                msg = json.loads(msg.data)
                if msg['type'] == 'restore_db':
                    async with create_task_group() as tg:
                        tg.start_soon(async_do_restore, msg['source'], messages_queue, msg['target'])
                        tg.start_soon(send_msg, messages_queue, ws)

        elif msg.type == aiohttp.WSMsgType.ERROR:
            logger.error('ws connection closed with exception %s' % ws.exception())

    logger.debug('websocket connection closed')
    return ws


def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    rac_logger = logging.getLogger('rac_tools')
    rac_logger.setLevel('INFO')

    app = web.Application()
    app.add_routes([
        web.get('/', handle),
        web.get('/ws', websocket_handler),
    ])

    web.run_app(app, port=8888)


if __name__ == '__main__':
    main()
