import argparse
import datetime
import logging
import os
from enum import Enum

import pyodbc
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class BackupType(Enum):
    full = 'D'
    diff = 'I'


class DataBase(BaseModel):
    driver: str = 'DRIVER={ODBC Driver 17 for SQL Server}'
    server: str
    port: int = 1433
    db: str = 'master'
    user: str = 's1_SQL'
    pw: str = '5felcy8yes'

    def get_connection_string(self):
        return ';'.join([self.driver, f'SERVER={self.server}', f'PORT={self.port}', f'DATABASE={self.db}',
                         f'UID={self.user}', f'PWD={self.pw}'])


def get_backup_path(conn, source_db, backup_type, backup_date):
    logger.debug('get_backup_path')
    cursor = conn.cursor()

    backup_start_date = backup_date.strftime('%Y-%m-%d 00:00:00')

    query = f'''
SELECT TOP (1) bmf.physical_device_name, bs.backup_finish_date
FROM   msdb.dbo.backupset AS bs LEFT OUTER JOIN msdb.dbo.backupmediafamily AS bmf ON bs.media_set_id = bmf.media_set_id
WHERE  (bs.database_name = N'{source_db}') AND (bs.type = '{backup_type.value}') 
AND (bs.backup_finish_date > CONVERT(DATETIME, '{backup_start_date}', 102))
ORDER BY bs.backup_finish_date DESC
    '''

    logger.debug(f'execute query {query}')

    cursor.execute(query)

    backup_path, backup_finish_date = cursor.fetchone()

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
    logger.debug('restore_db')
    logger.info('start restore BD')

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
    {no_recovery}  NOUNLOAD, REPLACE, STATS = 5
    '''

    diff_script = f"RESTORE DATABASE [{restored_base_name}] FROM  DISK = N'{dif_backup_path}' WITH  FILE = 1,  NOUNLOAD,  STATS = 5"

    if dif_backup_path:
        script = f'{script}{diff_script}'

    logger.debug(script)

    cursor.execute(script)

    # получаем ответ от сервера SQL и оповещаем о статусе выполнения
    while cursor.nextset():
        _, msg = cursor.messages[0]
        msg = msg.replace('[Microsoft][ODBC Driver 17 for SQL Server][SQL Server]', '')
        logger.info(msg)


def get_connection(db: DataBase):
    conn_str = db.get_connection_string()
    logger.debug(f'setup connection {conn_str}')
    base_conn = pyodbc.connect(conn_str)
    return base_conn


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    parser = argparse.ArgumentParser(description='Скрипт для перезаливки тестовой базы')
    parser.add_argument('source_db', help='имя базы источника в SQL')
    parser.add_argument('receiver_db', help='имя базы приемника в SQL')
    parser.add_argument('--source_server', default='pg-1c-01', help='имя источника сервера SQL')
    parser.add_argument('--receiver_server', default='pg-test-01', help='имя приемника сервера SQL')
    parser.add_argument('-v', '--verbose', choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'], default='INFO',
                        help='logging level')
    args = parser.parse_args()

    logger.setLevel(args.verbose)

    source_conn = get_connection(DataBase(server=args.source_server))

    source_db = args.source_db

    # TODO добавить проверку существования баз и серверов

    full_backup_path, full_backup_date = get_backup_path(source_conn, source_db, BackupType.full,
                                                         datetime.datetime.now())
    diff_backup_path, _ = get_backup_path(source_conn, source_db, BackupType.diff, full_backup_date)

    if not os.path.exists(full_backup_path):
        logger.error(f'File does not exist {full_backup_path}')
        return

    if not os.path.exists(diff_backup_path):
        logger.debug(f'File does not exist {diff_backup_path}')
        diff_backup_path = None

    receiver_conn = get_connection(DataBase(server=args.receiver_server))
    restored_db = args.receiver_db
    try:
        restore_db(receiver_conn, restored_db, full_backup_path, diff_backup_path)
    except pyodbc.ProgrammingError as e:
        logger.exception(e)
        logger.info('Error')
        return

    logger.info('DONE!')

    receiver_conn.close()
    source_conn.close()


if __name__ == '__main__':
    main()
