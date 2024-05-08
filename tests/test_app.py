import datetime
import os.path

import pandas as pd
import pytest
from app.api_client import ApiClient
from app.conf import settings
from app.data_handler import DataHandler
from app.conf import OutputFormats

ENDPOINTS = ['endpoint.csv', 'endpoint.json']


class TestApiClient:
    @pytest.fixture
    def client(self):
        return ApiClient()

    @pytest.mark.parametrize('endpoint', ENDPOINTS)
    def test_get_data(self, client, endpoint, start_date):
        res = client.get_data(endpoint, start_date)
        assert isinstance(res, pd.DataFrame)

    def test_throttle(self, client, start_date):
        try:
            client.get_data('throttling_endpoint', start_date)
            assert False
        except Exception as e:
            assert isinstance(e, RuntimeError)
            assert 'Max amount of retries' in str(e)

    def test_unknown_type(self, client, start_date):
        try:
            client.get_data('unknown_content_type', start_date)
        except Exception as e:
            assert isinstance(e, RuntimeError)
            assert 'Unknown content-type' in str(e)


class TestDataHandler:

    def test_dates(self):
        dh = DataHandler(ENDPOINTS)
        assert dh.start_date == datetime.date.today() - datetime.timedelta(days=6)
        assert dh.end_date == datetime.date.today()

    @pytest.mark.parametrize('endpoints', [ENDPOINTS, ENDPOINTS[0]])
    @pytest.mark.parametrize('output_format', [x for x in OutputFormats])
    def test_save_files(self, endpoints, output_format, start_date, end_date):
        settings.OUTPUT_FORMAT = output_format
        dh = DataHandler(endpoints, start_date, end_date)
        filename = dh.extract_data()
        assert isinstance(filename, str)
        assert os.path.isfile(filename)
        assert output_format.value in filename

    def test_file_exists(self):
        dh = DataHandler(ENDPOINTS)
        with open(dh.outfile, 'w') as f:
            f.write('')
        try:
            DataHandler(ENDPOINTS)
            assert False
        except Exception as e:
            assert isinstance(e, FileExistsError)
