import asyncio
import logging

import pyodbc
from anyio import to_thread

from services.rac_tools import get_infobase, BDInvalidName
from services.sql_tools import (
    get_connection, SQLServer, get_backup_path, BackupType, BackupFilesError,
    prepare_sql_query_for_restore
)

logger = logging.getLogger(__name__)


async def async_do_restore(source_path, messages_queue, target_path):
    messages_queue.put_nowait('START!')
    logger.debug('submit message START!')
    await asyncio.sleep(0)

    messages_queue.put_nowait('Получение информации о базе источнике')
    await asyncio.sleep(0)
    try:
        source_infobase = await to_thread.run_sync(get_infobase, source_path)
        messages_queue.put_nowait(f'база источник: {source_infobase}')
        logger.debug(f'submit message база источник: {source_infobase}')
        await asyncio.sleep(0)
    except (ChildProcessError, BDInvalidName, FileNotFoundError, ValueError) as e:
        messages_queue.put_nowait(str(e))
        logger.error(e)
        msg = 'Операция прервана!'
        messages_queue.put_nowait(msg)
        return

    messages_queue.put_nowait('Получение информации о базе приемнике')
    await asyncio.sleep(0)
    try:
        receiver_infobase = await to_thread.run_sync(get_infobase, target_path)
        messages_queue.put_nowait(f'база приемник: {receiver_infobase}')
        logger.debug(f'submit message база приемник: {receiver_infobase}')
        await asyncio.sleep(0)
    except (ChildProcessError, BDInvalidName, FileNotFoundError) as e:
        messages_queue.put_nowait(e)
        logger.error(e)
        msg = 'Операция прервана!'
        messages_queue.put_nowait(msg)
        return

    try:
        messages_queue.put_nowait(f'Получение путей файлов бекапа для базы: {source_infobase.db_name}')
        logger.debug(f'submit message Получение путей файлов бекапа для базы: {source_infobase.db_name}')
        await asyncio.sleep(0)

        with get_connection(SQLServer(server=source_infobase.db_server)) as source_conn:
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
    except pyodbc.OperationalError:
        msg = 'Сервер источник не найден или недоступен. Операция прервана!'
        messages_queue.put_nowait(msg)
        logger.error(msg)
        return
    except BackupFilesError:
        msg = 'Не удалось найти пути файлов бекапов. Операция прервана!'
        messages_queue.put_nowait(msg)
        logger.error(msg)
        return

    try:
        with get_connection(SQLServer(server=receiver_infobase.db_server)) as receiver_conn:
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
    except pyodbc.OperationalError:
        msg = 'Сервер приемник не найден или недоступен. Операция прервана!'
        messages_queue.put_nowait(msg)
        logger.error(msg)
        return
    except pyodbc.ProgrammingError:
        msg = 'БД приемник недоступна или не найдена. Операция прервана!'
        messages_queue.put_nowait(msg)
        logger.error(msg)
        return
    except FileNotFoundError:
        msg = 'Файлы бекапа не найдены на диске. Операция прервана!'
        messages_queue.put_nowait(msg)
        logger.error(msg)
        return

    messages_queue.put_nowait('DONE!')
    logger.info('DONE!')


def get_nextset(cursor):
    next_set = cursor.nextset()
    if not next_set:
        return

    _, msg = cursor.messages[0]
    msg = msg.replace('[Microsoft][ODBC Driver 17 for SQL Server][SQL Server]', '')
    logger.info(msg)
    return msg
