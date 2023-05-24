# import standard
import glob
import os
from typing import AnyStr, List, Tuple

# import third party
import pandas as pd

from lxml import html
# import own
from utils import DataSource
from .parser import Parser


class NasdaqCompanyDetailsParser(Parser):
    def parse_only_single_content(self, path):
        pass

    def __init__(self):
        self.config_path = 'data_collector/nasdaq/%s/Nasdaq_CompanyDetails/'
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         path_to_scrapped=self.config_path)
        self.processed_directory: str = ""
        self.nulls: int = 0

    def _parse_file(self, html_data: AnyStr, ticker: str) -> List:
        root = html.fromstring(html_data)

        attribute_select = dict(
            sector=f"//tbody[@class='summary-data__table-body']/tr[2]/td[2]/text()",
            industry=f"//tbody[@class='summary-data__table-body']/tr[3]/td[2]/text()",
            address=f"//address[@class='company-profile__contact']/a[1]/text()",
            telephone=f"//address[@class='company-profile__contact']/a[2]/text()",
            description=f"//span[@class='company-profile__description-excerpt "
                        f"company-profile__description-excerpt--ellipsis']/text()[1]",
            company_name="//span[@class='symbol-page-header__name']/text()")
        self.logger.info(f'PARSING COMPANY {ticker}')
        row = [ticker]

        for key in attribute_select:
            value = self._check_if_attr_exists(root=root, xpath=attribute_select[key],
                                               attribute=key)
            row.append(value)

        return row

    def parse_files(self) -> None:
        self.logger.info(f"STARTING PARSING RAW COMPANY DETAILS")
        parsed_companies_info = []

        headers = ['ticker', 'sector',
                   'industry', 'address',
                   'telephone', 'description',
                   'company_name']

        for ticker, data in self.get_next_file():
            path_ticker = self._attach_ticker_to_path(ticker=ticker)
            data = self._parse_file(html_data=data, ticker=ticker)
            df_ticker = pd.DataFrame(data=[data], columns=headers)
            DataSource.write_df_to_json(path=path_ticker, content=df_ticker)

        print(f"NULL {self.nulls}")
