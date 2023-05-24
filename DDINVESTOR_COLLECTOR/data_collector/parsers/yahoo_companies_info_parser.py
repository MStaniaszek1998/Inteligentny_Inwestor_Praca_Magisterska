# import standard
import glob
import os
from typing import AnyStr, Tuple, List

import lxml.html
# import third party
import pandas as pd
from tqdm import tqdm

# import own
from utils import DataSource
from .parser import Parser


class YahooCompaniesInfoParser(Parser):

    def __init__(self):
        self.config_path = "data_collector/yahoo/%s/Yahoo_CompanyDetails"
        super().__init__(logger_name=__name__,
                         logger_file_name=self.__class__.__name__,
                         path_to_scrapped=self.config_path)

        self.processed_directory: str = ""

    def _parse_file(self, html_data: AnyStr, ticker: str) -> List:
        self.logger.info(f"PARSING {ticker} ")
        root = lxml.html.fromstring(html_data)
        attribute_path = {
            'company_name': "/html/body/section/div[1]/div/h3/text()",
            'address': "/html/body/section/div[1]/div/div/p[1]/text()",
            'telephone': '/html/body/section/div[1]/div/div/p[1]/a[1]/text()',
            'website': '/html/body/section/div[1]/div/div/p[1]/a[2]/text()',
            'sector': '/html/body/section/div[1]/div/div/p[2]/span[2]/text()',
            'industry': '/html/body/section/div[1]/div/div/p[2]/span[4]/text()',
            'fulltime_employees': '/html/body/section/div[1]/div/div/p[2]/span[6]/span/text()',
            'description': '/html/body/section/section[2]/p/text()'
        }
        row = [ticker]
        for key in attribute_path:
            if key == 'address':
                value = self._check_if_attr_exists(root=root, xpath=attribute_path[key],
                                                   attribute=key, return_all_join=True)
            else:
                value = self._check_if_attr_exists(root=root, xpath=attribute_path[key],
                                                   attribute=key)
            row.append(value)

        return row

    def parse_files(self) -> None:
        self.logger.info(f"STARTING PARSING RAW COMPANY DETAILS")

        headers = ['ticker', 'company_name',
                   'address', 'telephone',
                   'website_url', 'sector',
                   'industry', 'no_employees',
                   'description']

        for ticker, data in tqdm(self.get_next_file()):
            path_ticker = self._attach_ticker_to_path(ticker=ticker)
            data_parsed = self._parse_file(html_data=data,ticker=ticker)
            df_parsed = pd.DataFrame(data=[data_parsed],columns=headers)
            DataSource.write_df_to_json(path=path_ticker, content=df_parsed)

