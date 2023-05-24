# import standard
import glob
import os
from datetime import datetime
from typing import Union, Optional, AnyStr, Dict

# import third-paty
import bs4
import pandas as pd
from tqdm import tqdm

# import own
from utils import DataSource
from .parser import Parser


class BizInsiderNewsLinksParser(Parser):

    def __init__(self):
        self.config_path = 'data_collector/business_insider/%s/BusinessInsider_News'
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         path_to_scrapped=self.config_path)
        self.processed_directory: str = ""

    def _parse_batch(self, batch: Dict[str, Union[AnyStr, datetime]]) -> pd.DataFrame:
        header = ['ticker',
                  'news_headline',
                  'source_url',
                  'collected_date',
                  'duration_time',
                  'publisher']

        rows = []
        soup = bs4.BeautifulSoup(batch['html_data'], features="lxml")
        for news in soup.findAll(class_='col-md-6 further-news-container latest-news-padding'):
            headline = news.a.text
            url = news.a.get('href')
            source_and_date = news.find('span', {'class': "warmGrey source-and-publishdate"}).text
            source = source_and_date[:source_and_date.rfind(" ")]
            duration_time = source_and_date[source_and_date.rfind(" "):]

            rows.append([batch['ticker'], headline, url, batch['collected_date'], duration_time,
                         source])

        df = pd.DataFrame(data=rows, columns=header)
        return df

    def parse_files(self) -> None:
        self.logger.info("STARTING PARSING RAW NEWS")
        batched_data = []
        for ticker, path in tqdm(self.get_next_file(return_path=True)):

            company_file_metadata = self._prepare_file_to_scrape(path=path, ticker=ticker)
            processed_data = self._parse_batch(batch=company_file_metadata)

            batched_data.append(processed_data)
        parsed_news = pd.concat(batched_data)
        parsed_news['ticker'] = parsed_news['ticker'].apply(lambda x: [x])
        parsed_news_grouped = parsed_news.groupby(['source_url', 'collected_date', 'duration_time',
                                                   'publisher', 'news_headline']).agg(
            {'ticker': 'sum'}).reset_index()
        DataSource.write_df_to_parquet(path=self.processed_directory, content=parsed_news_grouped)
        self.logger.info('ENDED PARSING FOR NEWS')
