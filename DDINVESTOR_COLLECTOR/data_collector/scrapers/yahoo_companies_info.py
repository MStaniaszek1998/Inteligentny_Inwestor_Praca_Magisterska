"""Sub-feed of the main feed - Yahoo
Scrapes the details about the companies and saves raw content into given folder"""
# import standard
import datetime

import pandas as pd
# import third-party
from tqdm import tqdm
from selenium.common import exceptions
# import own
from utils import make_daily_folder, DataSource, get_companies_to_scrape
from .base_scraper import BaseScraper


class YahooCompaniesInfo(BaseScraper):
    """Scrapes the details about company like:
    company name, sector,
    industry, number of employees,
    location, telephone and
    also meta data - company's website's url for the multitwittersource,
    threshold values are hardcoded values, which means the minimum byte size of the string to be
    saved. If the size is smaller it will not be saved"""

    def __init__(self):
        self.source_name = 'YahooCompaniesInfo'
        self.threshold_value = 11000
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         source_name=self.source_name, threshold_value=self.threshold_value)

    @make_daily_folder(path='data_collector/yahoo/%s/Yahoo_CompanyDetails')
    @get_companies_to_scrape
    def scrape_companies_info(self, today_path: str, company_list: pd.DataFrame) -> None:
        """Gets the company list to scrape from the tokenization system.
        Saves the scraped content for the coresponding daily folder given from today_path"""
        # Accept terms and conditions
        self.logger.info("START DOWNLOADING RAW DATA PROFILES")
        self.acquire_yahoo_access()

        for _, row in tqdm(company_list.iterrows(), total=company_list.shape[0]):
            now = datetime.datetime.now()
            self._set_token_status(row=row)
            url = row['url']
            self.logger.info("Downloading for %s", row['ticker_symbol'])
            try:
                self.driver.get(url)

                self.scroll_to_the_end_html()

                path_name = BaseScraper.create_ticker_path(path=today_path,
                                                           ticker=row['ticker_symbol'],
                                                           now=now)
                BaseScraper.sleep(1, 0.5)
                content = self.driver.find_element_by_xpath(
                    '//*[@id="Col1-0-Profile-Proxy"]').get_attribute('innerHTML')
            except Exception as e:
                self.logger.exception("DRIVER ERROR")
                self._set_record_status(row=row,
                                        status='failed')
                continue
            try:
                self.save_file(content=content, save_path=path_name,
                               do_threshold=True, row=row)

            except IOError:
                self.logger.exception("FAILED WRITTING TO FILE")
                self._set_record_status(row=row,
                                        status='failed')

        self.logger.info("END DOWNLOADING RAW DATA PROFILES")
