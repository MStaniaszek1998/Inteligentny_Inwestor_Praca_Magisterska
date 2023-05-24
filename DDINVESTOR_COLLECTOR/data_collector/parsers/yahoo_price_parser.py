# import standard
import glob

# import third party
import pandas as pd
from tqdm import tqdm

# import own
from utils import FileTypes, DataSource
from .parser import Parser


class YahooPriceParser(Parser):
    def parse_only_single_content(self, path):
        pass

    def __init__(self):
        self.config_path = 'data_collector/yahoo/%s/Yahoo_CompaniesPrice'
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         path_to_scrapped=self.config_path)

        self.processed_directory = str()

    def _parse_file(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        column_mapper = {"Date": "open_date",
                         "Open": "open_price",
                         "High": "high_price",
                         "Low": "low_price",
                         "Close": "close_price",
                         "Adj Close": "adj_close_price",
                         'Volume': 'volume',
                         "Ticker": 'ticker'}

        return dataframe.rename(columns=column_mapper)

    def parse_files(self) -> None:
        self.logger.info(f"STARTING PARSING RAW COMPANY DETAILS")
        parsed_prices = []

        for ticker, path in tqdm(self.get_next_file(file_type=FileTypes.csv, return_path=True)):
            ticker_path = self._attach_ticker_to_path(ticker=ticker)
            data = pd.read_csv(path)
            data_parsed = self._parse_file(dataframe=data)
            DataSource.write_df_to_csv(path=ticker_path, content=data_parsed)
