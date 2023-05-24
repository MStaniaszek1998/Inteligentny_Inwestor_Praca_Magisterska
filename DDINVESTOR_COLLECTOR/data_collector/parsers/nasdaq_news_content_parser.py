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


class NASDAQNewsContentParser(Parser):
    def parse_only_single_content(self, path):
        pass

    def __init__(self, parse_given_date: str = None):
        self.config_path = 'data_collector/nasdaq/%s/Nasdaq_NewsText/'
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         path_to_scrapped=self.config_path)
        self.processed_directory: str = ""
        self.nulls: int = 0

    def _parse_file(self, html_data: AnyStr) -> List:
        soup = BeautifulSoup(html_data, 'html.parser')
        title = self.find_in_soup(soup=soup, tag='h1',
                                  attributes={'class': 'article-header__headline'},
                                  key='title')
        content = self.find_in_soup(soup=soup, tag='div',
                                    attributes={'class': 'body__content'},
                                    key='content')
        contributor = self.find_in_soup(soup=soup, tag='div',
                                        attributes={'class': 'byline__info'},
                                        key='contributor')
        published_date = self.find_in_soup(soup=soup, tag='time',
                                           attributes={'class': 'timestamp__date'},
                                           key='published_date', value_to_get='datetime',
                                           get_only_text=False)

        mentioned_tickets = [x.text for x in
                             soup.find_all('a', {'class': 'topics-in-this-story__symbol'})]
        return [title, content, contributor, published_date, mentioned_tickets]

    def get_next_file(self, file_type: FileTypes = FileTypes.html,
                      return_path: bool = False) -> Tuple[str, AnyStr]:
        newest_folder, parsed_path = DataSource.get_config_paths(parsing_path=self.config_path)
        file_to_process = f"{newest_folder}/*.{file_type.name}"
        dir_path, filename = os.path.split(parsed_path)

        for path in glob.glob(file_to_process):
            _, filename = os.path.split(path)
            filename = filename.split(".")[0]
            path_to_save = os.path.join(dir_path, filename + ".json")
            content = DataSource.open_file(path=path)
            yield content, path_to_save

    def parse_files(self) -> None:
        self.logger.info(f"STARTING PARSING RAW NEWS CONTENT TO JSONs")

        headers = ['title', 'content',
                   'contributor', 'published_date',
                   'mentioned_tickers']

        for html_data, path_to_save in tqdm(self.get_next_file()):
            parsed_data = self._parse_file(html_data=html_data)
            df_parsed = pd.DataFrame(data=[parsed_data], columns=headers)
            DataSource.write_df_to_json(content=df_parsed, path=path_to_save)
        self.logger.info(f"ENDED PARSING RAW NEWS")
