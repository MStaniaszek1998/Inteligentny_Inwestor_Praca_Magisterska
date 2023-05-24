# import standard
import os
from datetime import datetime
# import third party
from typing import List

import pandas as pd
# import own
from selenium.webdriver.common.keys import Keys
from tqdm import tqdm

from scrapers.base_scraper import BaseScraper
from utils import make_daily_folder, DataSource


def get_companies_from_list(func):
    """Special POC of the database. It opens the file with the list of url news to scrape.
     Later it will be replaced by the table in the database"""

    def wrapper(*args, **kwargs):
        path_to_parsed = "data_collector/nasdaq/%s/Nasdaq_NewsLinks"
        scraped_path = DataSource.generate_mode_path(path=path_to_parsed)
        parsed_files = os.path.join(scraped_path, "parsed")
        newest_file = DataSource.get_newest_folder(path=parsed_files)
        df_news_list= DataSource.read_parquet_to_df(path=newest_file)
        df_news_list = df_news_list[df_news_list['ticker'].isin(['nvda', 'twtr', 'vz'])]
        kwargs['news_list'] = df_news_list
        return func(*args, **kwargs)

    return wrapper


class NASDAQNewsContent(BaseScraper):

    def __init__(self):
        self.source_name = 'NasdaqNewsLinks'
        self.threshold_value = 546582
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         source_name=self.source_name, threshold_value=self.threshold_value)



    @make_daily_folder(path="data_collector/nasdaq/%s/Nasdaq_NewsText")
    @get_companies_from_list
    def scrape_news_content(self, today_path: str, news_list: pd.DataFrame) -> None:
        self.logger.info("STARTING SCRAPPING NEWS CONTENTS")

        prefix_url = "https://www.nasdaq.com"
        filtered_companies = news_list.loc[news_list['ticker'].isin(['nvda', 'twtr', 'vz'])]
        for _, row in tqdm(filtered_companies.iterrows(), total=filtered_companies.shape[0]):

            now = datetime.now()

            url_to_scrape = prefix_url + row['news_url']
            BaseScraper.sleep(duration=1, variance=0.5)
            self.driver.get(url=url_to_scrape)
            self.scroll_to_the_end_html()
            news_name = self.create_news_template_name(news_url=row['news_url'], now=now)
            full_path = os.path.join(today_path, news_name + ".html")
            self.save_file_without_token(content=self.driver.page_source,
                                         save_path=full_path,
                                         do_threshold=False)

        self.logger.info("ENDED SCRAPPING NEWS CONTENTS")
