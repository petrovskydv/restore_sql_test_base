import logging
import re
import subprocess as sub

from pydantic import BaseModel, Field

logger = logging.getLogger(__name__)


class BDInvalidName(Exception):
    pass


class InfoBase(BaseModel):
    id: str = Field(alias='infobase')
    name: str
    db_server: str = Field(alias='db-server')
    db_name: str = Field(alias='db-name')


class RacClient:
    def __init__(self, exe_path=r'C:\Program Files (x86)\1cv8\8.3.19.1723\bin'):
        self.exe_path = exe_path

    def get_command(self, command, server_name, ras_port):
        return f'{self.exe_path}\\rac.exe {command} {server_name}:{ras_port}'


class Cluster1C:
    def __init__(self, host, rac_client: RacClient, ras_port=1545):
        self.rac_client = rac_client
        self.host = host
        self.port = ras_port
        self.id = None

    def _get_cluster_id(self) -> None:
        rac_method = 'cluster list'
        command = self.rac_client.get_command(rac_method, self.host, self.port)
        logger.debug(command)
        output = run_command(command)
        self.id = process_output(output, '')['cluster']
        logger.debug(f'cluster_id: {self.id}')

    def _get_infobases(self) -> dict[str, str]:
        rac_method = f'infobase summary list {self._cluster_suffix}'
        command = self.rac_client.get_command(rac_method, self.host, self.port)
        logger.debug(command)
        output = run_command(command)
        infobases = process_output(output, '')
        return {ib['name'].lower(): ib['infobase'] for ib in infobases}

    def get_infobase(self, infobase_name: str, username: str, pwd: str) -> InfoBase:
        self._get_cluster_id()
        infobases = self._get_infobases()
        logger.debug(f'{infobases=}')
        logger.debug(f'{infobase_name=}')
        ib_id = infobases.get(infobase_name)
        if not ib_id:
            raise BDInvalidName('Неверное имя базы 1с.')

        rac_method = f'infobase info --infobase={ib_id} ' \
                     f'--infobase-user="{username}" --infobase-pwd="{pwd}" {self._cluster_suffix}'
        command = self.rac_client.get_command(rac_method, self.host, self.port)
        logger.debug(command)
        output = run_command(command)
        return InfoBase.parse_obj(process_output(output, ''))

    @property
    def _cluster_suffix(self):
        return f'--cluster={self.id}'


def parse_infobase_connection_string(conn_string):
    server_name, infobase_name = re.findall(r'"([\w|\d|-]+)"', conn_string)
    logger.debug(f'{server_name=} {infobase_name=}')
    return server_name, infobase_name


def process_output(output, separator):
    objects = []
    is_new_object = True
    for row in output:
        if not row and row != separator:
            continue
        if (row.startswith(separator) and bool(separator)) or separator == row:
            is_new_object = True
            continue
        if is_new_object:
            obj = {}
            objects.append(obj)
            is_new_object = False
        key, value = row.replace(' ', '').split(':', maxsplit=1)
        obj[key] = value
    if len(objects) == 1:
        return objects[0]
    return objects


def run_command(command):
    try:
        proc = sub.Popen(command, stdout=sub.PIPE, stderr=sub.PIPE)
        outs, errs = proc.communicate()
        if errs:
            raise ChildProcessError(f'Error {errs.decode("cp866")}')
        else:
            output = outs.decode('cp866')

    except FileNotFoundError:
        raise FileNotFoundError('Неверный путь к клиенту rac')
    except Exception as exc:
        raise exc
    return output.split('\r\n')


def get_infobase(con_str):
    host_name, infobase_name = parse_infobase_connection_string(con_str)
    cluster = Cluster1C(host_name, RacClient())
    # todo вынести в настройки
    username = 'Петровский Денис'
    user_pwd = '0850'
    return cluster.get_infobase(infobase_name.lower(), username, user_pwd)


def main():
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    con_str = 'Srvr="pg-test-01";Ref="test_uppuusv";'
    try:
        infobase = get_infobase(con_str)
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
