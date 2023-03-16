import asyncio

import pyodbc

from rac_tools import get_infobase, BDInvalidName
from sql_tools import get_connection, SQLServer, get_backup_path, BackupType, BackupFilesError, async_restore_db


async def async_do_restore(login, messages_queue, password):
    messages_queue.put_nowait('DONE!')
    print(f'submit message DONE! ____')
    await asyncio.sleep(2)

    try:
        source_infobase = get_infobase(login)
        messages_queue.put_nowait(f'база источник_____: {source_infobase}')
        print(f'submit message база источник_____: {source_infobase}')
        await asyncio.sleep(0)
    except (ChildProcessError, BDInvalidName, FileNotFoundError) as e:
        messages_queue.put_nowait(e)
        return

    try:
        receiver_infobase = get_infobase(password)
        messages_queue.put_nowait(f'база приемник_____: {receiver_infobase}')
        print(f'submit message база приемник_____: {receiver_infobase}')
        await asyncio.sleep(0)
    except (ChildProcessError, BDInvalidName, FileNotFoundError) as e:
        messages_queue.put_nowait(e)
        return

    try:
        messages_queue.put_nowait(f'Получение путей файлов бекапа для базы: {source_infobase.db_name}')
        print(f'submit message Получение путей файлов бекапа для базы: {source_infobase.db_name}')

        with get_connection(SQLServer(server=source_infobase.db_server)) as source_conn:
            full_backup_path, full_backup_date = get_backup_path(source_conn, source_infobase.db_name)
            diff_backup_path, _ = get_backup_path(source_conn, source_infobase.db_name, BackupType.diff,
                                                  full_backup_date)
            print(f'{full_backup_path=}')
        await asyncio.sleep(0)
    except pyodbc.OperationalError:
        messages_queue.put_nowait('Сервер источник не найден или недоступен')
        return
    except BackupFilesError:
        messages_queue.put_nowait('Не удалось найти пути файлов бекапов')
        return

    try:
        messages_queue.put_nowait(f'Начало восстановления базы: {receiver_infobase.db_name}')
        print(f'submit message Начало восстановления базы: {receiver_infobase.db_name}')
        await asyncio.sleep(0)

        with get_connection(SQLServer(server=receiver_infobase.db_server)) as receiver_conn:
            await async_restore_db(receiver_conn, receiver_infobase.db_name, full_backup_path, diff_backup_path,
                                   messages_queue)
        await asyncio.sleep(0)
    except pyodbc.OperationalError:
        messages_queue.put_nowait('Сервер приемник не найден или недоступен')
        return
    except pyodbc.ProgrammingError:
        messages_queue.put_nowait('БД приемник недоступна или не найдена')
        return

    messages_queue.put_nowait('DONE!')
    print(f'submit message DONE! after 5')
