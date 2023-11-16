import asyncio
import datetime
import logging
import os.path

import dateutil.parser as dt_parser
from anyio import to_thread

from services.rac_tools import get_infobase, RacClient
from services.sql_tools import (
    SQLServer,
    BackupType,
    get_backup_path,
    get_connection,
    prepare_sql_query_for_restore,
    get_nextset
)

logger = logging.getLogger(__name__)


def put_log_msg(queue, msg):
    queue.put_nowait(msg)
    logger.debug(f'submit message: {msg}')


async def async_do_restore(messages_queue, source_path, target_path, raw_backup_date, settings):
    log_msg = 'START!'
    put_log_msg(messages_queue, log_msg)
    await asyncio.sleep(0)

    messages_queue.put_nowait('Получение информации о базе источнике')
    await asyncio.sleep(0)

    rac_client = RacClient(exe_path=settings.rac_path)
    source_infobase = await to_thread.run_sync(get_infobase, rac_client, source_path, settings.ib_username,
                                               settings.ib_user_pwd)
    log_msg = f'база источник: {source_infobase}'
    put_log_msg(messages_queue, log_msg)
    await asyncio.sleep(0)

    messages_queue.put_nowait('Получение информации о базе приемнике')
    await asyncio.sleep(0)
    receiver_infobase = await to_thread.run_sync(get_infobase, rac_client, target_path, settings.ib_username,
                                                 settings.ib_user_pwd)
    log_msg = f'база приемник: {receiver_infobase}'
    put_log_msg(messages_queue, log_msg)
    await asyncio.sleep(0)

    log_msg = f'Получение путей файлов бекапа для базы: {source_infobase.db_name}'
    put_log_msg(messages_queue, log_msg)
    await asyncio.sleep(0)

    backup_date = dt_parser.parse(raw_backup_date)
    diff_backup_date = None
    source_sql_server = SQLServer(server=source_infobase.db_server, user=settings.sql_user, pw=settings.sql_user_pwd)
    with get_connection(source_sql_server) as source_conn:
        full_backup_path, full_backup_date = await to_thread.run_sync(
            get_backup_path,
            source_conn,
            source_infobase.db_name,
            BackupType.full,
            backup_date
        )
        diff_backup_path, diff_backup_date = await to_thread.run_sync(
            get_backup_path,
            source_conn,
            source_infobase.db_name,
            BackupType.diff,
            backup_date
        )
        if not os.path.exists(diff_backup_path):
            logger.debug('Нет файла диф бекапа')
            diff_backup_path = None
            diff_backup_date = datetime.datetime(2000,1,1)
        logger.debug(f'{full_backup_path=} {full_backup_date=}')
        logger.debug(f'{diff_backup_path=} {diff_backup_date=}')
    await asyncio.sleep(0)

    backup_date = max(full_backup_date, diff_backup_date)
    log_msg = f'\nБаза будет восстановлена на  {backup_date.strftime("%H:%M:%S %d.%m.%Y")}\n'
    put_log_msg(messages_queue, log_msg)

    target_sql_server = SQLServer(server=receiver_infobase.db_server, user=settings.sql_user, pw=settings.sql_user_pwd)
    with get_connection(target_sql_server) as receiver_conn:
        script = await to_thread.run_sync(
            prepare_sql_query_for_restore,
            receiver_conn,
            full_backup_path,
            receiver_infobase.db_name,
            diff_backup_path,
        )
        log_msg = f'Начало восстановления {source_infobase.db_name} ===> {receiver_infobase.db_name}'
        put_log_msg(messages_queue, log_msg)

        await asyncio.sleep(0)

        cursor = receiver_conn.cursor()
        # устанавливаем режим автосохранения транзакций
        receiver_conn.autocommit = True
        cursor.execute(script)
        while True:
            msg = await to_thread.run_sync(get_nextset, cursor)
            if not msg:
                break
            messages_queue.put_nowait(msg)
            await asyncio.sleep(0)

    await asyncio.sleep(0)

    log_msg = 'DONE!'
    put_log_msg(messages_queue, log_msg)
