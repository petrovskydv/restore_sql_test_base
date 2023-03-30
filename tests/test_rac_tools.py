from unittest.mock import MagicMock

from services.rac_tools import Cluster1C, RacClient


def test_get_infobases():
    host_name = 'pg-test-01'
    cluster = Cluster1C(host_name, RacClient())
    cluster._get_cluster_id = MagicMock(return_value=1)
    assert cluster._get_cluster_id() == 1
    cluster._get_infobases = MagicMock(return_value=1)
    assert cluster._get_infobases() == 1
