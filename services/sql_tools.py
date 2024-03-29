import datetime
import logging
import os
from contextlib import contextmanager
from enum import Enum

import pyodbc
from pydantic import BaseModel

from services.exceptions import BackupFilesError

logger = logging.getLogger(__name__)


class BackupType(Enum):
    full = 'D'
    diff = 'I'


class SQLServer(BaseModel):
    driver: str = 'DRIVER={ODBC Driver 17 for SQL Server}'
    server: str
    user: str
    pw: str
    port: int = 1433
    db: str = 'master'

    def get_connection_string(self):
        return ';'.join([self.driver, f'SERVER={self.server}', f'PORT={self.port}', f'DATABASE={self.db}',
                         f'UID={self.user}', f'PWD={self.pw}'])


def get_backup_path(conn, source_db, backup_type=BackupType.full, backup_date=datetime.datetime.now()):
    logger.debug('get_backup_path')
    cursor = conn.cursor()

    backup_start_date = backup_date.strftime('%Y-%m-%d %H:%M:%S')

    query = f'''
SELECT TOP (1) bmf.physical_device_name, bs.backup_finish_date
FROM   msdb.dbo.backupset AS bs LEFT OUTER JOIN msdb.dbo.backupmediafamily AS bmf ON bs.media_set_id = bmf.media_set_id
WHERE  (bs.database_name = N'{source_db}') AND (bs.type = '{backup_type.value}')
AND (bs.backup_finish_date <= CONVERT(DATETIME, '{backup_start_date}', 102)) AND bs.is_snapshot=0
ORDER BY bs.backup_finish_date DESC'''

    logger.debug(f'execute query {query}')

    cursor.execute(query)
    response = cursor.fetchone()

    if not response and backup_type == BackupType.full:
        raise BackupFilesError

    logger.debug(response)    

    backup_path, backup_finish_date = response
    logger.debug(f'backup_path {backup_type}: {backup_path}')
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
    logger.info('start restore BD')

    cursor = conn.cursor()

    # устанавливаем режим автосохранения транзакций
    conn.autocommit = True

    script = prepare_sql_query_for_restore(conn, full_backup_path, restored_base_name, dif_backup_path)

    cursor.execute(script)

    # получаем ответ от сервера SQL и оповещаем о статусе выполнения
    while cursor.nextset():
        _, msg = cursor.messages[0]
        msg = msg.replace('[Microsoft][ODBC Driver 17 for SQL Server][SQL Server]', '')
        logger.info(msg)


def prepare_sql_query_for_restore(conn, full_backup_path, restored_base_name, dif_backup_path=None):
    # получаем логические имена файлов и их пути для целевой базы
    data_file, log_file = get_files_names(conn, restored_base_name)
    data_file_name, data_file_path = data_file
    log_file_name, log_file_path = log_file
    data_file_path = get_right_path(data_file_path, restored_base_name)
    log_file_path = get_right_path(log_file_path, f'{restored_base_name}_log')

    no_recovery = ''
    diff_script = ''

    if dif_backup_path:
        no_recovery = 'NORECOVERY,'
        diff_script = f"RESTORE DATABASE [{restored_base_name}] FROM  DISK = N'{dif_backup_path}' " \
                      f"WITH  FILE = 1,  NOUNLOAD,  STATS = 5"

    script = f'''
        USE [master]
        ALTER DATABASE [{restored_base_name}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE
        RESTORE DATABASE [{restored_base_name}] FROM
        DISK = N'{full_backup_path}' WITH  FILE = 1,
        MOVE N'{data_file_name}' TO N'{data_file_path}',
        MOVE N'{log_file_name}' TO N'{log_file_path}',
        {no_recovery}  NOUNLOAD, REPLACE, STATS = 5
        '''

    script = f'{script}{diff_script}'

    logger.debug(script)
    return script


def get_right_path(data_file_path, restored_base_name):
    file_path, _ = os.path.split(data_file_path)
    _, ext = os.path.splitext(data_file_path)
    return os.path.join(file_path, f'{restored_base_name}{ext}')


@contextmanager
def get_connection(db: SQLServer):
    conn_str = db.get_connection_string()
    logger.debug(f'setup connection {conn_str}')
    base_conn = None
    try:
        base_conn = pyodbc.connect(conn_str, timeout=2)
        yield base_conn
    finally:
        if base_conn:
            base_conn.close()


def get_nextset(cursor):
    next_set = cursor.nextset()
    if not next_set:
        return

    _, msg = cursor.messages[0]
    msg = msg.replace('[Microsoft][ODBC Driver 17 for SQL Server][SQL Server]', '')
    logger.info(msg)
    return msg
