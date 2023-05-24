"""sub-feed of the main feed - Yahoo
Downloades using API the prices for each company in csv format, and saves the data to the given
daily folder."""
# import standard
import random
import time
from datetime import datetime
from io import StringIO

import pandas as pd
import requests
# import third-party
from dateutil.relativedelta import relativedelta
from tqdm import tqdm

# import own
from utils import make_daily_folder, get_companies_to_scrape, DataSource
from .base_scraper import BaseScraper


class YahooCompaniesPrice(BaseScraper):
    """data_scraper
    Special case of data scraper,which doesn't use selenium because it uses Yahoo website api
    to download the company's stock prices either for current day or history (today subtract 10
    years default). It's easier to get companies stock prices by downloading, rather than
    scrapping the
    same information and then parsing it into csv.
    It gets: Open date,open price,
            Low price, High price,
            Close price, and volume for each trading day"""

    def __init__(self):
        self.source_name = 'YahooCompaniesHistoryPrice'
        super().__init__(logger_name=__name__,
                         logger_file_name=self.__class__.__name__, use_selenium=False,
                         source_name=self.source_name)

    @make_daily_folder(path='data_collector/yahoo/%s/Yahoo_CompaniesPrice')
    @get_companies_to_scrape
    def download_price(self, today_path: str, company_list: pd.DataFrame,
                       do_scrape_history: bool = False, start_periode: str = None,
                       years_behind: int = 10) -> None:
        """Downloads price data for each company given the list from the tokenization system.
        And saves all ready csv-s into the indicated path. """
        self.logger.info('STARTED DOWNLOADING CSVs')

        end_period = datetime.now()

        if do_scrape_history:
            year_ago = end_period - relativedelta(years=years_behind)
            start_period = int(datetime.timestamp(year_ago))
        else:
            if start_periode is None:
                start_periode = datetime.now()
                year, month, day = start_periode.year, start_periode.month, start_periode.day
            else:
                year, month, day = start_periode.split('-')

            date_from_string = datetime(year=int(year), day=int(day), month=int(month))
            start_period = int(datetime.timestamp(date_from_string))

        end_period = int(datetime.timestamp(end_period))
        for _, row in tqdm(company_list.iterrows(), total=company_list.shape[0]):
            now = datetime.now()
            self._set_token_status(row=row)

            url = f"{row['url']}period1={start_period}&period2={end_period}&interval=1d" \
                  f"&events=history "

            BaseScraper.sleep(1, 0.5)

            self.logger.info("DOWNLOADING CSV FOR %s", row['ticker_symbol'])

            response = requests.get(url)

            if response.status_code != 200:
                self.logger.warning("MISSED REQUEST FOR TICKER %s", row['ticker_symbol'])
                self._set_record_status(row=row,
                                        status='failed')
                continue
            df_downloaded = pd.read_csv(StringIO(response.text), sep=',')
            df_downloaded['ticker_symbol'] = row['ticker_symbol']

            path_name = BaseScraper.create_ticker_path(path=today_path,
                                                       ticker=row['ticker_symbol'],
                                                       now=now,
                                                       file_ext='csv')

            DataSource.write_df_to_csv(path=path_name, content=df_downloaded)
            self._set_record_status(row=row,
                                    status='success')

        self.logger.info('ENDED DOWNLOADING')
