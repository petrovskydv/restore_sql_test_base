import asyncio
import logging

import pyodbc

from services.rac_tools import get_infobase, BDInvalidName
from services.sql_tools import get_connection, SQLServer, get_backup_path, BackupType, BackupFilesError, async_restore_db

logger = logging.getLogger(__name__)


async def async_do_restore(source_path, messages_queue, target_path):
    messages_queue.put_nowait('START!')
    logger.debug(f'submit message START!')
    await asyncio.sleep(0)

    messages_queue.put_nowait('Получение информации о базе источнике')
    await asyncio.sleep(0)
    try:
        source_infobase = get_infobase(source_path)
        messages_queue.put_nowait(f'база источник: {source_infobase}')
        logger.debug(f'submit message база источник: {source_infobase}')
        await asyncio.sleep(0)
    except (ChildProcessError, BDInvalidName, FileNotFoundError) as e:
        messages_queue.put_nowait(e)
        logger.error(e)
        return

    messages_queue.put_nowait('Получение информации о базе приемнике')
    await asyncio.sleep(0)
    try:
        receiver_infobase = get_infobase(target_path)
        messages_queue.put_nowait(f'база приемник: {receiver_infobase}')
        logger.debug(f'submit message база приемник: {receiver_infobase}')
        await asyncio.sleep(0)
    except (ChildProcessError, BDInvalidName, FileNotFoundError) as e:
        messages_queue.put_nowait(e)
        logger.error(e)
        return

    try:
        messages_queue.put_nowait(f'Получение путей файлов бекапа для базы: {source_infobase.db_name}')
        logger.debug(f'submit message Получение путей файлов бекапа для базы: {source_infobase.db_name}')
        await asyncio.sleep(0)

        with get_connection(SQLServer(server=source_infobase.db_server)) as source_conn:
            full_backup_path, full_backup_date = get_backup_path(source_conn, source_infobase.db_name)
            diff_backup_path, _ = get_backup_path(source_conn, source_infobase.db_name, BackupType.diff,
                                                  full_backup_date)
            logger.debug(f'{full_backup_path=}')
            logger.debug(f'{diff_backup_path=}')
        await asyncio.sleep(0)
    except pyodbc.OperationalError:
        msg = 'Сервер источник не найден или недоступен'
        messages_queue.put_nowait(msg)
        logger.error(msg)
        return
    except BackupFilesError:
        msg = 'Не удалось найти пути файлов бекапов'
        messages_queue.put_nowait(msg)
        logger.error(msg)
        return

    try:
        messages_queue.put_nowait(f'Начало восстановления базы: {receiver_infobase.db_name}')
        logger.debug(f'submit message Начало восстановления базы: {receiver_infobase.db_name}')
        await asyncio.sleep(0)

        with get_connection(SQLServer(server=receiver_infobase.db_server)) as receiver_conn:
            await async_restore_db(receiver_conn, receiver_infobase.db_name, full_backup_path, diff_backup_path,
                                   messages_queue)
        await asyncio.sleep(0)
    except pyodbc.OperationalError:
        msg = 'Сервер приемник не найден или недоступен'
        messages_queue.put_nowait(msg)
        logger.error(msg)
        return
    except pyodbc.ProgrammingError:
        msg = 'БД приемник недоступна или не найдена'
        messages_queue.put_nowait(msg)
        logger.error(msg)
        return
    except FileNotFoundError:
        msg = 'Файлы бекапа не найдены на диске'
        messages_queue.put_nowait(msg)
        logger.error(msg)

    messages_queue.put_nowait('DONE!')
    logger.info('DONE!')
