# import standard
import glob
import os
from typing import AnyStr, List, Tuple

# import third party
import pandas as pd
from bs4 import BeautifulSoup

from lxml import html
# import own
from tqdm import tqdm

from utils import DataSource, FileTypes
from .parser import Parser


class BizInsiderNewsContentParser(Parser):

    def __init__(self, parse_given_date: str = None):
        self.config_path = 'data_collector/business_insider/%s/BusinessInsider_NewsText'
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         path_to_scrapped=self.config_path)
        self.processed_directory: str = ""
        self.nulls: int = 0

    def _parse_file(self, html_data: AnyStr, ticker: str = None) -> List:
        soup = BeautifulSoup(html_data, 'html.parser')
        title = self.find_in_soup(soup=soup, tag='h1',
                                  attributes={'class': 'article-title'},
                                  key='title')
        content = self.find_in_soup(soup=soup, tag='div',
                                    attributes={'class': 'col-xs-12 news-content no-padding'},
                                    key='content')
        publisher = self.find_in_soup(soup=soup, tag='div',
                                      attributes={'class': 'news-post-source'},
                                      key='contributor')
        published_date = self.find_in_soup(soup=soup, tag='span',
                                           attributes={'class': 'news-post-quotetime warmGrey'},
                                           key='published_date')

        return [title, content,published_date,publisher,ticker]

    def get_next_file(self, file_type: FileTypes = FileTypes.html,
                      return_path: bool = False) -> Tuple[str, AnyStr]:
        newest_folder, parsed_path = DataSource.get_config_paths(parsing_path=self.config_path)
        file_to_process = f"{newest_folder}/*.{file_type.name}"
        dir_path, filename = os.path.split(parsed_path)

        for path in glob.glob(file_to_process):
            _, filename = os.path.split(path)
            filename = filename.split(".")[0]
            filename_without_tickers = "_".join(filename.split("_")[1:])
            ticker_symbol = filename.split("_")[0]
            path_to_save = os.path.join(dir_path, filename_without_tickers + ".json")
            content = DataSource.open_file(path=path)
            yield ticker_symbol, content, path_to_save

    def parse_files(self) -> None:
        self.logger.info(f"STARTING PARSING RAW NEWS CONTENT TO JSONs")

        headers = ['title', 'content',
                   'published_date', 'publisher',
                   'ticker_symbol']

        for ticker_symbol, html_data, path_to_save in tqdm(self.get_next_file()):
            parsed_data = self._parse_file(html_data=html_data, ticker=ticker_symbol)
            df_parsed = pd.DataFrame(data=[parsed_data], columns=headers)
            DataSource.write_df_to_json(content=df_parsed, path=path_to_save)
        self.logger.info(f"ENDED PARSING RAW NEWS")
