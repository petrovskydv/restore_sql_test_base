import asyncio
import logging
from asyncio import Queue
from pathlib import Path

import aiohttp
from aiohttp import web

from service import async_do_restore

BASE_DIR = Path(__file__).resolve().parent


async def handle(request):
    with open('index1.html', 'r') as f:
        file = f.read()

    return web.Response(body=file, headers={
        'Content-Type': 'text/html',
    })


async def do_login(request):
    data = await request.post()
    login = data['login']
    password = data['password']
    # print(login, password)
    # запуск восстановления базы с очередью

    messages_queue = request.app['q']

    # thread = Thread(target=do_restore, args=(login, messages_queue, password))
    # asyncio.r
    # func_call = functools.partial(asyncio.to_thread, do_restore, login, messages_queue, password)
    # coro = asyncio.to_thread(do_restore, login, messages_queue, password)

    # async with create_task_group() as tg:
    #     tg.start_soon(func_call)
    # asyncio.create_task(asyncio.to_thread(do_restore, login, messages_queue, password))
    asyncio.create_task(async_do_restore(login, messages_queue, password))

    with open('status.html', 'r') as f:
        file = f.read()

    return web.Response(body=file, headers={
        'Content-Type': 'text/html',
    })


async def websocket_handler(request):
    print('websocket_handler')
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    messages_queue = request.app['q']
    await ws.send_str('START____')
    while True:
        message = await messages_queue.get()
        print(f'get message______________{message}')
        await ws.send_str(message)
        print(f'send message______________{message}')

    async for msg in ws:
        if msg.type == aiohttp.WSMsgType.TEXT:
            if msg.data == 'close':
                await ws.close()
            else:
                await ws.send_str(msg.data + '/answer')
        elif msg.type == aiohttp.WSMsgType.ERROR:
            print('ws connection closed with exception %s' %
                  ws.exception())

    print('websocket connection closed')

    # return ws


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    app = web.Application()
    app.add_routes([
        web.get('/', handle),
        web.get('/ws', websocket_handler),
        web.post('/status', do_login)
    ])

    messages_queue = Queue()
    app['q'] = messages_queue

    web.run_app(app, port=8888)


if __name__ == '__main__':
    main()
