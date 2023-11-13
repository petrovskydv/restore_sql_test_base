import logging

from services.rac_tools import get_infobase
from services.exceptions import BDInvalidName
from settings import settings


def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    con_str = 'Srvr="pg-test-01";Ref="test_uppuusv";'
    try:
        infobase = get_infobase(con_str, settings.ib_username, settings.ib_user_pwd)
        print(infobase)
        print(infobase.db_server, infobase.db_name)
    except ChildProcessError as e:
        print(e)
    except BDInvalidName as e:
        print(e)
    except FileNotFoundError as e:
        print(e)


if __name__ == '__main__':
    main()
