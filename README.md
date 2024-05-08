# ETL (extract transform load) for solar/wind endpoints

This project provides an ETL tool to extract, transform, and save data from various API endpoints. The data is saved in
different formats based on configuration settings.

## Features

- **Data Extraction**: Extracts data from one or multiple API endpoints over a specified date range.
- **Data Transformation**: Cleans and standardizes the data, ensuring that all timestamps are in UTC format.
- **Data Loading**: Saves the processed data to an output file in CSV, Parquet, or JSON Lines format.
- **Data Merging**: It's possible to merge data from different API endpoints into single table

## Configuration

The tool used **PydanticSettings** for configuration

Config options can be found in file **app/conf.py**. All the options can be set from environment variables.

## Setup Instructions

1. Clone this repository.
2. Install requirements

```
pip install -r requirements.txt
```

3. Set `DATA_SOURCE_BASE_URL` environment variable to URL of the server (default is `http://localhost:8000/`)
4. Set `API_KEY` environment variable with valid API key
5. Start app with `python main.py`

## Custom Usage

There are two options of using `DataHandler` depending on the use-case.

#### Merge data from different sources:

Pass sources (endpoints) as a `list` to constructor to get data from all endpoints merged in single file

```python
import datetime
from app.data_handler import DataHandler

handler = DataHandler(
    endpoint=["/api/endpoint1", "/api/endpoint2"],
    start_date = datetime.date(2024, 5, 1),
    end_date = datetime.date(2024, 5, 7)
)
handler.extract_data()
```

#### Keep data from different source separately.

In this case need to construct separate object for each source. E.g.:
```python
import datetime
from app.data_handler import DataHandler

# first
handler = DataHandler(
    endpoint="/api/endpoint1",
)
handler.extract_data()

# second

handler = DataHandler(
    endpoint="/api/endpoint2",
)
handler.extract_data()
```

### Testing
App has 100% test coverage and uses mocks for testing. 
To run all tests with coverage report:
```
pytest --cov=app --cov-fail-under 100 --cov-report term-missing -vv -x -s
```
Also it can be tested in integration mode with real server if `--integration` flag is passed.


### Notes
- The ETL tool will skip the extraction process if the output file already exists to prevent overwriting. 
- If an overwrite is desired, the existing file should be deleted manually or an exception handler should handle the FileExistsError.
- If dates are not provided to constructor - it will take the data from the **last week**


