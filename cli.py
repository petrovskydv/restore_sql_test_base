import argparse
import datetime
import logging
import os

import pyodbc

from services.rac_tools import get_infobase, BDInvalidName
from services.sql_tools import BackupFilesError, SQLServer, get_backup_path, restore_db, get_connection, BackupType
from settings import settings

logger = logging.getLogger('db_restore')


def get_args():
    parser = argparse.ArgumentParser(description='Скрипт для перезаливки тестовой базы')
    parser.add_argument('source_db', help='имя базы источника в SQL')
    parser.add_argument('receiver_db', help='имя базы приемника в SQL')
    parser.add_argument('--source_server', default='pg-1c-01', help='имя источника сервера SQL')
    parser.add_argument('--receiver_server', default='pg-test-01', help='имя приемника сервера SQL')
    parser.add_argument('-v', '--verbose', choices=['CRITICAL', 'ERROR', 'WARNING', 'INFO', 'DEBUG'], default='INFO',
                        help='logging level')
    args = parser.parse_args()
    return args


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    args = get_args()

    logger.setLevel(args.verbose)
    logging.getLogger('rac_tools').setLevel(args.verbose)

    try:
        logger.debug(args.source_db)
        source_infobase = get_infobase(args.source_db, settings.ib_username, settings.ib_user_pwd)
    except (ChildProcessError, BDInvalidName, FileNotFoundError) as e:
        logger.error(e)
        return

    try:
        receiver_infobase = get_infobase(args.receiver_db, settings.ib_username, settings.ib_user_pwd)
    except (ChildProcessError, BDInvalidName, FileNotFoundError) as e:
        logger.error(e)
        return

    try:
        with get_connection(SQLServer(server=source_infobase.db_server)) as source_conn:
            full_backup_path, full_backup_date = get_backup_path(source_conn, source_infobase.db_name, BackupType.full,
                                                                 datetime.datetime.now())
            diff_backup_path, _ = get_backup_path(source_conn, source_infobase.db_name, BackupType.diff,
                                                  full_backup_date)
    except pyodbc.OperationalError:
        logger.error('Сервер источник не найден или недоступен')
        return
    except BackupFilesError:
        logger.error('Не удалось найти пути файлов бекапов')
        return

    if not os.path.exists(full_backup_path):
        logger.error(f'File does not exist {full_backup_path}')
        return

    if not os.path.exists(diff_backup_path):
        logger.debug(f'File does not exist {diff_backup_path}')
        diff_backup_path = None

    try:
        with get_connection(SQLServer(server=receiver_infobase.db_server)) as receiver_conn:
            restore_db(receiver_conn, receiver_infobase.db_name, full_backup_path, diff_backup_path)
    except pyodbc.OperationalError:
        logger.error('Сервер приемник не найден или недоступен')
        return
    except pyodbc.ProgrammingError:
        logger.error('БД приемник недоступна или не найдена')
        return

    logger.info('DONE!')


if __name__ == '__main__':
    main()
