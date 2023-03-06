import datetime
import os
from enum import Enum

import pyodbc


class BackupType(Enum):
    full = 'D'
    diff = 'I'


def get_backup_path(conn, source_db, backup_type, backup_date):
    cursor = conn.cursor()

    backup_start_date = backup_date.strftime('%Y-%m-%d 00:00:00')

    query = f'''
SELECT TOP (1) bmf.physical_device_name, bs.backup_finish_date
FROM   msdb.dbo.backupset AS bs LEFT OUTER JOIN msdb.dbo.backupmediafamily AS bmf ON bs.media_set_id = bmf.media_set_id
WHERE  (bs.database_name = N'{source_db}') AND (bs.type = '{backup_type.value}') 
AND (bs.backup_finish_date > CONVERT(DATETIME, '{backup_start_date}', 102))
ORDER BY bs.backup_finish_date DESC
    '''

    cursor.execute(query)

    backup_path, backup_finish_date = cursor.fetchone()

    return backup_path, backup_finish_date


def restore_db(conn, restored_base_name, full_backup_path, dif_backup_path=None):
    cursor = conn.cursor()

    # устанавливаем режим автосохранения транзакций
    conn.autocommit = True

    stats = 0

    # TODO получить пути к файлам в которые восстанавливать

    # TODO получить логические имена файлов

    script = f'''
    USE [master]
    ALTER DATABASE [{restored_base_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
    RESTORE DATABASE [{restored_base_name}] FROM  
    DISK = N'{full_backup_path}' WITH  FILE = 1,  
    MOVE N'S1v82_UppBuFmG' TO N'D:\\SQLDB\\test_uppbufmg.mdf',  
    MOVE N'S1v82_UppBuFmG_log' TO N'D:\\SQLDB\\test_uppbufmg_log.ldf',  
    NORECOVERY,  NOUNLOAD,  STATS = 5
    RESTORE DATABASE [{restored_base_name}] FROM  
    DISK = N'{dif_backup_path}' WITH  FILE = 1,  NOUNLOAD,  STATS = 5
    '''
    # print(script)

    cursor.execute(script)

    # получаем ответ от сервера SQL и оповещаем о статусе выполнения
    while cursor.nextset():
        stats += 1
        # if stats > 0:
        # print(f'Выполненно {stats}% - {now_time()}')
        _, msg = cursor.messages[0]
        print(msg)


def get_connection(server_name, server_port, user_name, password):
    driver = 'DRIVER={ODBC Driver 17 for SQL Server}'
    server = f'SERVER={server_name}'
    port = f'PORT={server_port}'
    db = 'DATABASE=master'
    user = f'UID={user_name}'
    pw = f'PWD={password}'

    conn_str = ';'.join([driver, server, port, db, user, pw])
    base_conn = pyodbc.connect(conn_str)
    return base_conn


def main():
    prod_conn = get_connection(
        server_name='pg-1c-01',
        server_port=1433,
        user_name='s1_SQL',
        password='5felcy8yes'
    )

    test_server_conn = get_connection(
        server_name='pg-test-01',
        server_port=1433,
        user_name='s1_SQL',
        password='5felcy8yes'
    )

    source_db = 'S1v82_UppBuFmG'

    full_backup_path, full_backup_date = get_backup_path(prod_conn, source_db, BackupType.full, datetime.datetime.now())
    diff_backup_path, _ = get_backup_path(prod_conn, source_db, BackupType.diff, full_backup_date)
    print(full_backup_path, full_backup_date)
    print(diff_backup_path, _)

    # TODO проверить что файлы существуют

    restored_db = 'test_uppbufmg'
    restore_db(test_server_conn, restored_db, full_backup_path, diff_backup_path)


if __name__ == '__main__':
    main()
