# import standard
import glob
import os
from typing import AnyStr, List

# import third party
import pandas as pd

from lxml import html
# import own
from utils import DataSource
from .parser import Parser


class MultiSourceTwitterUrlsParser(Parser):

    def __init__(self):
        self.config_path = "data_collector/multisource/%s/MultiSource_TwitterUrls"
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         path_to_scrapped=self.config_path)
        self.processed_directory: str = ""
        self.to_update_sql = """
                UPDATE CompaniesStatus 
                SET url = %s
                WHERE subfeed='Twitter' and ticker_symbol=%s
                """

    def parse_twitter_url(self, data: AnyStr = None, ticker: str = None) -> List:
        """Special case of parsing. It uses xpath search to look for all possible twitter urls
        from the main web page of the company. Later it return ticker with associated twitter url"""
        root = html.fromstring(data)
        url_xpath = "//a[contains(@href,'twitter')]"
        twitter_url = self._check_if_attr_exists(xpath=url_xpath, root=root, attribute="url")
        try:
            twitter_url = twitter_url.get('href')

            return [ticker, twitter_url]
        except AttributeError:
            return [ticker, None]

    def parse_files(self):
        self.logger.info(f"STARTING PARSING RAW MAIN WEB PAGES")
        rows_parsed = []
        headers = ['ticker', 'twitter_url']
        for ticker, data in self.get_next_file():
            rows_parsed.append(self.parse_twitter_url(data=data,
                                                      ticker=ticker))

        df = pd.DataFrame(data=rows_parsed,
                          columns=headers)
        percentage_of_nulls = (df['twitter_url'].isna().sum() / df.shape[0]) * 100
        self.logger.info("Percent of null values %s", percentage_of_nulls)
        self.logger.info("Amount of Null values %s", df['twitter_url'].isna().sum())

        DataSource.write_df_to_parquet(path=self.processed_directory, content=df)
