# import standard
import glob
import os
from typing import AnyStr, List, Tuple, Dict

# import third party
import bs4
import pandas as pd

from lxml import html
# import own
from utils import DataSource
from .parser import Parser


class BizInsiderCompaniesInfoParser(Parser):
    def parse_only_single_content(self, path):
        pass

    def __init__(self):
        self.config_path = 'data_collector/business_insider/%s/BusinessInsider_CompanyDetails'
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         path_to_scrapped=self.config_path)
        self.to_update_sql = """
        UPDATE companiesstatus 
        SET url = %s
        WHERE ticker_symbol=%s and subfeed='MainWebsite'
        """
        self.processed_directory: str = ""
        self.nulls: int = 0

    def _parse_shareholder_info(self, html_data: AnyStr) -> Dict[str, str]:
        soup = bs4.BeautifulSoup(html_data, features='lxml')

        table = soup.find("div", {'class': 'table-responsive'})
        shareholders_percent = dict()
        shareholders = []
        percent = []
        for row in table.find_all('tr'):
            cols = row.find_all('td')
            cols = [x.text.strip() for x in cols]
            if len(cols) < 1:
                continue
            if (cols[0] is not None) & (cols[1] is not None):
                shareholders.append(cols[0])
                percent.append(cols[1])
        shareholders_percent['shareholders_name'] = shareholders
        shareholders_percent['percentage'] = percent
        return shareholders_percent

    def _parse_file(self, html_data: AnyStr, ticker: str) -> List:
        root = html.fromstring(html_data)
        root_address = '//div[2]/div[1]/div[2]/table/tbody'

        attribute_select = dict(
            address=f"{root_address}/tr[1]/td/text()",
            postoffice_box=f"{root_address}/tr[2]/td[2]/text()",
            telephone=f"{root_address}/tr[3]/td[2]/text()",
            fax=f"{root_address}/tr[4]/td[2]/text()",
            website=f"{root_address}/tr[5]/td[2]/a/text()",
            description='//div[@class="content"]/text()',
            company_name='//h1[@class="box-headline"]/text()')
        self.logger.info(f'PARSING COMPANY {ticker}')
        row = [ticker]

        for key in attribute_select:

            value = self._check_if_attr_exists(root=root, xpath=attribute_select[key],
                                               attribute=key)
            if value is None:
                self.nulls += 1
            if value is not None and key == 'company_name':
                value = value[:value.rfind('-')].strip()
            row.append(value)

        shareholder_percent = self._parse_shareholder_info(html_data=html_data)
        row.append(shareholder_percent)
        return row

    def parse_files(self) -> None:
        self.logger.info(f"STARTING PARSING RAW COMPANY DETAILS")
        parsed_companies_info = []

        headers = ['ticker', 'address',
                   'postoffice_box', 'telephone',
                   'fax', 'website',
                   'description', 'company_name', 'shareholders']

        for ticker, data in self.get_next_file():
            path_ticker = self._attach_ticker_to_path(ticker=ticker)
            data_parsed = self._parse_file(html_data=data, ticker=ticker)
            df_parsed = pd.DataFrame(data=[data_parsed], columns=headers)
            DataSource.write_df_to_json(path=path_ticker, content=df_parsed)
        print(f"NULL {self.nulls}")
