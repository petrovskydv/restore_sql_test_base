import datetime
import os
from enum import Enum

import pyodbc

SERVER_PORT = 1433,
USER_NAME = 's1_SQL',
PASSWORD = '5felcy8yes'


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


def get_files_names(conn, source_db) -> list:
    cursor = conn.cursor()

    query = f'''
SELECT name, physical_name FROM [{source_db}].[sys].[database_files]
ORDER BY type
    '''

    cursor.execute(query)
    logical_name_files = cursor.fetchall()

    return logical_name_files


def restore_db(conn, restored_base_name, full_backup_path, dif_backup_path=None):
    cursor = conn.cursor()

    # устанавливаем режим автосохранения транзакций
    conn.autocommit = True

    # получаем логические имена файлов и их пути для целевой базы
    data_file, log_file = get_files_names(conn, restored_base_name)
    data_file_name, data_file_path = data_file
    log_file_name, log_file_path = log_file

    no_recovery = 'NORECOVERY,' if dif_backup_path else ''

    script = f'''
    USE [master]
    ALTER DATABASE [{restored_base_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
    RESTORE DATABASE [{restored_base_name}] FROM  
    DISK = N'{full_backup_path}' WITH  FILE = 1,  
    MOVE N'{data_file_name}' TO N'{data_file_path}',  
    MOVE N'{log_file_name}' TO N'{log_file_path}',  
    {no_recovery}  NOUNLOAD,  STATS = 5
    '''

    diff_script = f"RESTORE DATABASE [{restored_base_name}] FROM  DISK = N'{dif_backup_path}' WITH  FILE = 1,  NOUNLOAD,  STATS = 5"

    if dif_backup_path:
        script = f'{script}{diff_script}'

    print(script)

    cursor.execute(script)

    # получаем ответ от сервера SQL и оповещаем о статусе выполнения
    while cursor.nextset():
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
        server_port=SERVER_PORT,
        user_name=USER_NAME,
        password=PASSWORD
    )

    test_server_conn = get_connection(
        server_name='pg-test-01',
        server_port=SERVER_PORT,
        user_name=USER_NAME,
        password=PASSWORD
    )

    source_db = 'S1v82_UppBuFmG'

    full_backup_path, full_backup_date = get_backup_path(prod_conn, source_db, BackupType.full, datetime.datetime.now())
    diff_backup_path, _ = get_backup_path(prod_conn, source_db, BackupType.diff, full_backup_date)

    if not os.path.exists(full_backup_path):
        print(f'File does not exist {full_backup_path}')
        return

    if not os.path.exists(diff_backup_path):
        print(f'File does not exist {diff_backup_path}')
        diff_backup_path = None

    restored_db = 'test_uppbufmg'
    restore_db(test_server_conn, restored_db, full_backup_path, diff_backup_path)


if __name__ == '__main__':
    main()
