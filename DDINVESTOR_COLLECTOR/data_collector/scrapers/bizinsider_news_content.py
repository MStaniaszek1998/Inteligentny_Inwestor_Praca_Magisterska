# import standard
"""Sub-feed of the main feed - Business Insider,
It scrapes either all of the news pages only from domain markets.businessinsider.com"""
from datetime import datetime
# import own
import os
from typing import Tuple

import pandas as pd
# import third-party
from tqdm import tqdm
from  random import randint
from utils import make_daily_folder, DataSource, get_companies_to_scrape
from .base_scraper import BaseScraper


def get_companies_from_list(func):
    """Special POC of the database. It opens the file with the list of url news to scrape.
     Later it will be replaced by the table in the database"""

    def wrapper(*args, **kwargs):
        path_to_parsed = "data_collector/business_insider/%s/BusinessInsider_News"
        scraped_path = DataSource.generate_mode_path(path=path_to_parsed)
        parsed_files = os.path.join(scraped_path, "parsed")
        newest_file = DataSource.get_newest_folder(path=parsed_files).split(".")[0]

        df_news_list = DataSource.read_parquet_to_df(path=newest_file)
        df_news_list_select = df_news_list.loc[df_news_list['ticker'].isin(['nvda', 'twtr', 'vz'])]
        stocks_filter = df_news_list_select['source_url'].str.contains("/news/stocks")
        bonds_filter = df_news_list_select['source_url'].str.contains("/news/bonds")
        kwargs['news_list'] = df_news_list_select[bonds_filter | stocks_filter]

        return func(*args, **kwargs)

    return wrapper


class BizInsiderNewsContent(BaseScraper):
    """Scrapes the news content for the given url page"""

    def __init__(self):
        self.source_name = 'BizInsiderNewsText'
        self.threshold_value = 154970
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         source_name=self.source_name, threshold_value=self.threshold_value)

    @make_daily_folder(path='data_collector/business_insider/%s/BusinessInsider_NewsText')
    @get_companies_from_list
    def download_news_content(self, today_path: str = None, news_list: pd.DataFrame = None) -> None:
        """News_Content for given company"""
        self.logger.info("STARTING SCRAPPING NEWS CONTENTS")

        prefix_url = "https://markets.businessinsider.com"
        for _, row in tqdm(news_list.iterrows(),total=news_list.shape[0]):

            now = datetime.now()

            url_to_scrape = prefix_url + row['source_url']
            BaseScraper.sleep(duration=randint(1, 10), variance=0.5)
            try:
                self.driver.get(url=url_to_scrape)
            except Exception as e:
                self.logger.exception(f"ERROR WHILE GETTING PAGE: {row['url']}")
            self.scroll_to_the_end_html()
            news_name = self.create_news_template_name(news_url=row['source_url'], now=now)
            news_name = f"{row['ticker']}_{news_name}"
            full_path = os.path.join(today_path, news_name + ".html")
            self.save_file_without_token(content=self.driver.page_source,
                                         save_path=full_path,
                                         do_threshold=False)

        self.logger.info("ENDED SCRAPPING NEWS CONTENTS")
