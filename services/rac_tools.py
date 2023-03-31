import logging
import re
import subprocess as sub
from typing import Dict

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
    # todo предусмотреть смену версии платформы
    def __init__(self, exe_path=r'C:\Program Files (x86)\1cv8\8.3.19.1723\bin'):
        self.exe_path = exe_path

    def _get_command(self, command, server_name, ras_port):
        return f'{self.exe_path}\\rac.exe {command} {server_name}:{ras_port}'

    @staticmethod
    def _run_command(command):
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

    @staticmethod
    def _process_output(output, separator=''):
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

    def get(self, command, server_name, ras_port):
        command = self._get_command(command, server_name, ras_port)
        logger.debug(command)
        output = self._run_command(command)
        return self._process_output(output)


class Cluster1C:
    def __init__(self, host, rac_client: RacClient, ras_port=1545):
        self.rac_client = rac_client
        self.host = host
        self.port = ras_port
        self.id = None

    def _get_cluster_id(self) -> None:
        rac_method = 'cluster list'
        response = self.rac_client.get(rac_method, self.host, self.port)
        self.id = response['cluster']
        logger.debug(f'cluster_id: {self.id}')

    def _get_infobases(self) -> Dict[str, str]:
        rac_method = f'infobase summary list {self._cluster_suffix}'
        infobases = self.rac_client.get(rac_method, self.host, self.port)
        return {ib['name'].lower(): ib['infobase'] for ib in infobases}

    def get_infobase(self, infobase_name: str, username: str, pwd: str) -> InfoBase:
        infobases = self._get_infobases()
        logger.debug(f'{infobases=}')
        logger.debug(f'{infobase_name=}')
        ib_id = infobases.get(infobase_name)
        if not ib_id:
            raise BDInvalidName('Неверное имя базы 1с.')

        rac_method = f'infobase info --infobase={ib_id} ' \
                     f'--infobase-user="{username}" --infobase-pwd="{pwd}" {self._cluster_suffix}'
        response = self.rac_client.get(rac_method, self.host, self.port)
        return InfoBase.parse_obj(response)

    @property
    def _cluster_suffix(self):
        if not self.id:
            self._get_cluster_id()
        return f'--cluster={self.id}'


# todo покрыть тестом
def parse_infobase_connection_string(conn_string):
    server_name, infobase_name = re.findall(r'"([\w|\d|-]+)"', conn_string)
    logger.debug(f'{server_name=} {infobase_name=}')
    return server_name, infobase_name


def get_infobase(con_str, ib_username, ib_user_pwd):
    host_name, infobase_name = parse_infobase_connection_string(con_str)
    cluster = Cluster1C(host_name, RacClient())
    return cluster.get_infobase(infobase_name.lower(), ib_username, ib_user_pwd)



