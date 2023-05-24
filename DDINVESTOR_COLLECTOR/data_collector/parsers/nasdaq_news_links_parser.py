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


class NASDAQNewsLinksParser(Parser):
    def parse_only_single_content(self, path):
        pass

    def __init__(self, parse_given_date: str = None):
        self.config_path = 'data_collector/nasdaq/%s/Nasdaq_NewsLinks/'
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         path_to_scrapped=self.config_path)
        self.processed_directory: str = ""
        self.nulls: int = 0

    def _parse_file(self, html_data: AnyStr, ticker: str) -> List:
        root = html.fromstring(html_data)

        attribute_select = dict(
            title='.//p[@class="quote-news-headlines__item-title"]/span/text()',
            news_url='.//a[@class="quote-news-headlines__link"]',
            published_date=".//span[@class='quote-news-headlines__date']/text()")

        self.logger.info(f'PARSING COMPANY {ticker}')

        rows = []
        for news in root.xpath(".//li[@class='quote-news-headlines__item']"):
            row = [ticker]
            for key in attribute_select:

                if key == "news_url":
                    value = self._check_if_attr_exists(root=news, xpath=attribute_select[key],
                                                       attribute=key).get('href')
                    row.append(value)
                else:
                    value = self._check_if_attr_exists(root=news, xpath=attribute_select[key],
                                                       attribute=key)
                    row.append(value)

            rows.append(row)
        return rows

    def parse_files(self) -> None:
        self.logger.info(f"STARTING PARSING RAW NEWS LINKS")
        parsed_all_data = []

        headers = ['ticker', 'title',
                   'news_url', 'published_date']

        for ticker, path in self.get_next_file(return_path=True):
            company_file_metadata = self._prepare_file_to_scrape(path=path, ticker=ticker)
            data = company_file_metadata['html_data']

            parsed_data = self._parse_file(html_data=data, ticker=ticker)
            df_parsed = pd.DataFrame(data=parsed_data, columns=headers)
            df_parsed['collected_date'] = company_file_metadata['collected_date']
            parsed_all_data.append(df_parsed)

        parsed_news_links = pd.concat(parsed_all_data)
        dedup = parsed_news_links.drop_duplicates(subset="news_url")
        DataSource.write_df_to_parquet(path=self.processed_directory, content=dedup)
