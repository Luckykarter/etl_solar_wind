import os.path
from typing import Union
import logging

from app.api_client import ApiClient
import datetime
import pandas as pd
from app.conf import settings, OutputFormats


def parse_timestamp(value: Union[int, str]):
    """
    support both milliseconds and ISO formats
    """
    if isinstance(value, int):
        return pd.to_datetime(value, unit='ms', utc=True)
    return pd.to_datetime(value, utc=True)


class DataHandler:
    """
    Responsible to handle one portion of data from one or many sources between dates and save it to the folder
    """

    def __init__(
            self,
            endpoint: Union[str, list],
            start_date: datetime.date = None,
            end_date: datetime.date = None
    ):
        """
        :param endpoint: endpoint of target service. If list provided - data from several endpoints is merged
        :param start_date: if not provided defaults to a week from end_date
        :param end_date: if not provided defaults to today
        """
        if end_date is None:
            end_date = datetime.date.today()
        if start_date is None:
            start_date = end_date - datetime.timedelta(days=6)

        self.client = ApiClient()
        if isinstance(endpoint, str):
            endpoint = [endpoint]
        self.endpoints = endpoint
        self.start_date = start_date
        self.end_date = end_date
        self.outfile = self.get_output_filename()

        # Parquet format is not appendable - so need to concat in-memory
        # If dataset becomes large - partitioned Parquet can be used
        self.concat_in_memory = settings.OUTPUT_FORMAT == OutputFormats.PARQUET

        if os.path.exists(self.outfile):
            raise FileExistsError(f"File {self.outfile} already exists. You may want to delete it first.")

    def clean_and_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Column names with some convention
        df.columns = ["naive_timestamp", "variable", "value", "last_modified_utc"]

        # Convert timestamps with timezone aware UTC format
        df["naive_timestamp"] = df["naive_timestamp"].apply(parse_timestamp)
        df["last_modified_utc"] = df["last_modified_utc"].apply(parse_timestamp)

        # Convert to appropriate types
        df["variable"] = pd.to_numeric(df["variable"])
        df["value"] = pd.to_numeric(df["value"])

        return df

    def save_dataframe(self, df: pd.DataFrame):
        if settings.OUTPUT_FORMAT == OutputFormats.PARQUET:
            df.to_parquet(self.outfile, index=False)
        elif settings.OUTPUT_FORMAT == OutputFormats.CSV:
            df.to_csv(self.outfile, mode='a', index=False, header=not os.path.isfile(self.outfile))
        elif settings.OUTPUT_FORMAT == OutputFormats.JSONL:
            df.to_json(self.outfile, mode='a', lines=True, orient='records', index=False)

    def get_output_filename(self) -> str:
        """
        :return: e.g.: /output/endpoint1_endpoint2/2024/05/Data_01-05_07-05.csv
        """
        source_name = "_".join([x.replace('/', '_') for x in self.endpoints])

        path = os.path.join(
            settings.OUTPUT_FOLDER,
            source_name,
            str(self.end_date.year),
            str(self.end_date.month))
        os.makedirs(path, exist_ok=True)
        filename = (f'Data_{self.start_date.strftime("%m-%d")}_'
                    f'{self.end_date.strftime("%m-%d")}.{settings.OUTPUT_FORMAT.value}')
        return os.path.join(path, filename)

    def extract_data(self) -> str:
        logging.info(f"Getting data from {self.endpoints} for the period of {self.start_date} - {self.end_date}")
        start = self.start_date
        frames = []
        while start < self.end_date:
            for endpoint in self.endpoints:
                df = self.client.get_data(endpoint, start)
                df = self.clean_and_transform(df)
                df['data_source'] = endpoint
                if self.concat_in_memory:
                    frames.append(df)
                else:
                    self.save_dataframe(df)
            start += datetime.timedelta(days=1)
        if self.concat_in_memory:
            df = pd.concat(frames, ignore_index=True)
            self.save_dataframe(df)
        logging.info(f"Data saved in: {self.outfile}")
        return self.outfile
