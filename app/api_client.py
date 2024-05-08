import datetime
import time

import requests
from requests.exceptions import ConnectionError
from requests.models import Response
from app.conf import settings
from urllib.parse import urljoin
from http import HTTPStatus
import logging
import pandas as pd
from io import StringIO


class ApiClient:
    def __request_with_retries(self, method: str, url: str, attempt=0, **kwargs) -> Response:
        """
        handles connection issues and requests throttling
        """
        if attempt >= settings.CONNECTION_RETRIES:
            raise RuntimeError("Max amount of retries exceeded")

        try:
            res = requests.request(method, url, **kwargs)
        except ConnectionError as e:  # pragma: no cover
            logging.error(e)
            time.sleep(settings.BACKOFF_FACTOR ** attempt)
            return self.__request_with_retries(method, url, attempt=attempt + 1, **kwargs)

        if res.status_code == HTTPStatus.TOO_MANY_REQUESTS:
            retry_after = int(res.headers.get('Retry-After', settings.BACKOFF_FACTOR ** attempt))
            logging.warning(f'Too many requests for {url} retrying in: '
                            f'{retry_after} sec. Attempt {attempt + 1}/{settings.CONNECTION_RETRIES}')

            time.sleep(retry_after)
            return self.__request_with_retries(method, url, attempt + 1, **kwargs)
        res.raise_for_status()
        return res

    def make_request(self, method: str, endpoint: str, **kwargs) -> Response:
        url = urljoin(settings.DATA_SOURCE_BASE_URL, endpoint)
        params = kwargs.pop('params', {})
        params['api_key'] = settings.API_KEY
        return self.__request_with_retries(method, url, params=params, **kwargs)

    def to_dataframe(self, res: Response) -> pd.DataFrame:
        """
        Converts data from HTTP response to DataFrame
        :param res: HTTP response
        :return: Pandas DataFrame
        """
        content_type = res.headers.get('content-type')
        if content_type == 'application/json':
            df = pd.DataFrame(res.json())
        elif content_type == 'text/csv':
            df = pd.read_csv(StringIO(res.text))
        else:
            raise RuntimeError(f"Unknown content-type {content_type} for URL {res.url}")
        return df

    def get_data(self, endpoint: str, requested_date: datetime.date) -> pd.DataFrame:
        res = self.make_request('GET', f'{requested_date.isoformat()}/{endpoint}')
        return self.to_dataframe(res)
