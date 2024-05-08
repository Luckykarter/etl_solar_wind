import datetime
import shutil

import pytest
from app.conf import settings


def pytest_addoption(parser):
    parser.addoption('--integration', action='store_true', default=False, help='If run tests in integration mode')


@pytest.fixture(scope='session')
def start_date() -> datetime.date:
    return datetime.date(2024, 1, 1)


@pytest.fixture(scope='session')
def end_date(start_date) -> datetime.date:
    return start_date + datetime.timedelta(days=6)


@pytest.fixture(scope='session', autouse=True)
def is_integration(pytestconfig) -> bool:
    return bool(pytestconfig.getoption('integration'))


@pytest.fixture(scope='session', autouse=True)
def assert_settings(is_integration):
    if not is_integration:
        settings.API_KEY = 'dummy'
        settings.DATA_SOURCE_BASE_URL = 'http://mock-url/'
        settings.CONNECTION_RETRIES = 1
        settings.BACKOFF_FACTOR = 0.1
    yield
    # cleanup after session - removes data folder
    shutil.rmtree(settings.OUTPUT_FOLDER)


@pytest.fixture(scope='session', autouse=True)
def mocker(is_integration: bool):
    """
    returns mocker that can be used to mock requests
    in case of running in integration - returns dummy mocker
    """
    if is_integration:
        class Mocker:
            def get(self, *args, **kwargs):
                pass

            # add post/put/delete etc... if needed
            def stop(self):
                pass

        mocker = Mocker()
    else:
        from requests_mock import Mocker
        mocker = Mocker()
        mocker.start()
    yield mocker
    mocker.stop()


@pytest.fixture(scope='session', autouse=True)
def add_mocks(mocker, start_date, end_date):
    while start_date < end_date:
        mocker.get(f'http://mock-url/{start_date.isoformat()}/endpoint.csv', text="""
    Naive_Timestamp , Variable,value,Last Modified utc
    2024-05-08 00:00:00+00:00,782,-20.38147391333479,2024-05-08 00:00:00+00:00
    2024-05-08 00:05:00+00:00,203,-20.97809826513145,2024-05-08 00:00:00+00:00
    2024-05-08 00:10:00+00:00,743,33.92993618742469,2024-05-08 00:00:00+00:00
    2024-05-08 00:15:00+00:00,148,-47.96034520598836,2024-05-08 00:00:00+00:00
        """, headers={"Content-Type": 'text/csv'})
        mocker.get(f'http://mock-url/{start_date.isoformat()}/endpoint.json', json=[
            {
                "Naive_Timestamp ": 1609459200000,
                " Variable": 406,
                "value": -11.4135058739,
                "Last Modified utc": 1609459200000
            },
            {
                "Naive_Timestamp ": 1609459500000,
                " Variable": 801,
                "value": 11.1174452434,
                "Last Modified utc": 1609459200000
            }], headers={"Content-Type": 'application/json'})
        start_date += datetime.timedelta(days=1)

    mocker.get('http://mock-url/2024-01-01/throttling_endpoint', status_code=429)
    mocker.get('http://mock-url/2024-01-01/unknown_content_type', text="")

