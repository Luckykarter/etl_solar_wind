from app.data_handler import DataHandler
import datetime

if __name__ == '__main__':
    # There are two approaches of getting the data from sources
    sources = ['renewables/windgen.csv', 'renewables/solargen.json']

    # 1. Separated by source (each source will be in separate folder)
    for source in sources:
        dh = DataHandler(source)
        dh.extract_data()

    # 2. Merged into single table - list of sources passed
    dh = DataHandler(sources)
    dh.extract_data()
