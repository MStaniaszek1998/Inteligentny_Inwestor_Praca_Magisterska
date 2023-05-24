# import standard

import glob
import itertools
import os
from typing import AnyStr, List

import pandas as pd
# import third party
from bs4 import BeautifulSoup
from tqdm import tqdm

# import own
from utils import DataSource
from .parser import Parser


class BizInsiderIndexesParser(Parser):
    def parse_only_single_content(self, path):
        pass

    def __init__(self):
        self.config_path = "data_collector/business_insider/%s/Businessinsider_CompanyList"
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         path_to_scrapped=self.config_path)
        self._not_found_companies: int = 0
        self.to_insert_sql_query: str = """INSERT INTO CompaniesStatus(ticker_symbol,name,subfeed) 
        VALUES(%s,%s,'MainWebsite')"""
        self.processed_directory: str = ""

    def get_next_file(self) -> str:
        newest_folder, parsed_path = DataSource.get_config_paths(parsing_path=self.config_path)
        self.processed_directory = parsed_path
        files_to_iterate = f"{newest_folder}/*.html"
        for path in glob.glob(files_to_iterate):
            content = DataSource.open_file(path)
            yield content

    def _parse_file(self, file: AnyStr) -> List:
        soup = BeautifulSoup(file, features="lxml")
        table = soup.select('#index-list-container > div.table-responsive > table')[0] \
            .find('tbody') \
            .find_all('tr')

        page_row = []
        for row in table:
            company = row.find('td')
            if company is None:
                self._not_found_companies += 1
            else:
                company_name = company.get_text()
                company_name = company_name[1:].strip()
                company_ticker = company.find('a')['href']
                company_ticker = company_ticker[company_ticker.rfind('/') + 1:company_ticker
                    .rfind('-')]

                page_row.append([company_name, company_ticker])
        return page_row

    def parse_files(self) -> None:
        pages = []
        headers = ['name', 'ticker']
        for file in tqdm(self.get_next_file()):
            per_page_rows = self._parse_file(file)
            pages.append(per_page_rows)

        merged_pages = list(itertools.chain.from_iterable(pages))

        companies = pd.DataFrame(merged_pages,
                                 columns=headers)
        DataSource.write_df_to_parquet(path=self.processed_directory, content=companies)
