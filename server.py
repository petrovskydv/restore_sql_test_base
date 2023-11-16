import json
import logging
import os.path
from asyncio import Queue
from pathlib import Path

import aiohttp
import pyodbc
from aiohttp import web
from anyio import create_task_group

from services.exceptions import BDInvalidName, BackupFilesError
from services.service import async_do_restore
from settings import settings

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
            # logger.debug(f'send msg to browser {message}')
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
        if msg.type == aiohttp.WSMsgType.ERROR:
            logger.error('ws connection closed with exception %s' % ws.exception())
            continue

        if msg.data == 'close':
            await ws.close()
            continue

        msg = json.loads(msg.data)
        if msg['type'] != 'restore_db':
            continue
        try:
            print(msg)
            async with create_task_group() as tg:
                tg.start_soon(
                    async_do_restore,
                    messages_queue,
                    msg['source'],
                    msg['target'],
                    msg['backup_date'],
                    settings
                )
                tg.start_soon(send_msg, messages_queue, ws)
        except (ChildProcessError, BDInvalidName, FileNotFoundError, ValueError) as e:
            msg = f'Что-то пошло не так \n {str(e)}'
            await log_send_msg(msg, ws)
            logger.exception(e)
        except pyodbc.OperationalError:
            msg = 'Сервер не найден или недоступен. Операция прервана!'
            await log_send_msg(msg, ws)
        except BackupFilesError:
            msg = 'Не удалось найти пути файлов бекапов. Операция прервана!'
            await log_send_msg(msg, ws)
        except pyodbc.ProgrammingError:
            msg = 'БД приемник недоступна. Операция прервана!'
            await log_send_msg(msg, ws)
        except FileNotFoundError:
            msg = 'Файлы бекапа не найдены на диске. Операция прервана!'
            await log_send_msg(msg, ws)

    logger.debug('websocket connection closed')
    return ws


async def log_send_msg(msg, ws):
    await ws.send_str(msg)
    await ws.send_str('Операция прервана!')
    logger.error(msg)


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
