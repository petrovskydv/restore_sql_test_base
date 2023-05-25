import asyncio
import logging

from anyio import to_thread

from services.rac_tools import get_infobase
from services.sql_tools import (
    SQLServer,
    BackupType,
    get_backup_path,
    get_connection,
    prepare_sql_query_for_restore,
    get_nextset
)

logger = logging.getLogger(__name__)


async def async_do_restore(source_path, messages_queue, target_path, settings):
    messages_queue.put_nowait('START!')
    logger.debug('submit message START!')
    await asyncio.sleep(0)

    messages_queue.put_nowait('Получение информации о базе источнике')
    await asyncio.sleep(0)
    source_infobase = await to_thread.run_sync(get_infobase, source_path, settings.ib_username, settings.ib_user_pwd)
    messages_queue.put_nowait(f'база источник: {source_infobase}')
    logger.debug(f'submit message база источник: {source_infobase}')
    await asyncio.sleep(0)

    messages_queue.put_nowait('Получение информации о базе приемнике')
    await asyncio.sleep(0)
    receiver_infobase = await to_thread.run_sync(get_infobase, target_path, settings.ib_username, settings.ib_user_pwd)
    messages_queue.put_nowait(f'база приемник: {receiver_infobase}')
    logger.debug(f'submit message база приемник: {receiver_infobase}')
    await asyncio.sleep(0)

    messages_queue.put_nowait(f'Получение путей файлов бекапа для базы: {source_infobase.db_name}')
    logger.debug(f'submit message Получение путей файлов бекапа для базы: {source_infobase.db_name}')
    await asyncio.sleep(0)

    source_sql_server = SQLServer(server=source_infobase.db_server, user=settings.sql_user, pw=settings.sql_user_pwd)
    with get_connection(source_sql_server) as source_conn:
        full_backup_path, full_backup_date = await to_thread.run_sync(
            get_backup_path,
            source_conn,
            source_infobase.db_name
        )
        diff_backup_path, _ = await to_thread.run_sync(
            get_backup_path,
            source_conn,
            source_infobase.db_name,
            BackupType.diff,
            full_backup_date
        )
        logger.debug(f'{full_backup_path=}')
        logger.debug(f'{diff_backup_path=}')
        # todo добавить вывод даты и времени когда был сделан этот бекап
    await asyncio.sleep(0)

    target_sql_server = SQLServer(server=receiver_infobase.db_server, user=settings.sql_user, pw=settings.sql_user_pwd)
    with get_connection(target_sql_server) as receiver_conn:
        script = await to_thread.run_sync(
            prepare_sql_query_for_restore,
            receiver_conn,
            full_backup_path,
            receiver_infobase.db_name,
            diff_backup_path,
        )
        messages_queue.put_nowait(f'Начало восстановления базы: {receiver_infobase.db_name}')
        logger.debug(f'submit message Начало восстановления базы: {receiver_infobase.db_name}')
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

    messages_queue.put_nowait('DONE!')
    logger.info('DONE!')
