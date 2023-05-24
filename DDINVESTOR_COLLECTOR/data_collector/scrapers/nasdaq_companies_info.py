"""Sub-feed of the main feed - NASDAQ
Scrapes the detail information about the company information from the nasdaq stock market website.
It includes the information about shares of stackholders and who are the Executives in the
company etc."""
# import standard
import datetime

import pandas as pd
from selenium.common import exceptions
# import third-party
from tqdm import tqdm

from utils import make_daily_folder, DataSource, get_companies_to_scrape
# import own
from .base_scraper import BaseScraper


class NASDAQCompaniesInfo(BaseScraper):
    """Scrapes the details about the company. Receives the list of urls to scrape from
    tokenization system and at the end saves raw htmls"""

    def __init__(self):
        self.source_name = 'NASDAQCompaniesInfo'
        self.threshold_value = 546582
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         source_name=self.source_name, threshold_value=self.threshold_value)

    @make_daily_folder(path="data_collector/nasdaq/%s/Nasdaq_CompanyDetails")
    @get_companies_to_scrape
    def scrape_companies_info(self, today_path: str, company_list: pd.DataFrame) -> None:
        """Gets the list to scrape from the system. Goes to the end of the web page,
        renders content. Next clicks the "Show more" button to get a detail description of the
        company which also includes url to some administration documents."""
        self.logger.info("STARTING DOWNLOADING COMPANIES INFO")



        for _, row in tqdm(company_list.iterrows(), total=company_list.shape[0]):
            now = datetime.datetime.now()
            self._set_token_status(row=row)
            ticker = row['ticker_symbol']
            url = row['url']

            path_name = BaseScraper.create_ticker_path(path=today_path,
                                                       ticker=ticker,
                                                       now=now)
            try:
                self.driver.get(url)
                BaseScraper.sleep(1, 0.5)
                self.logger.debug('DOWNLOADING FOR %s', row["ticker_symbol"])

                self.scroll_to_the_end_html()
                BaseScraper.sleep(1, 0.5)
            except exceptions.WebDriverException:
                self.logger.exception("NO DOWNLOADED CONTENT FOR %s", row['ticker_symbol'])
                self._set_record_status(row=row,
                                        status='failed')
                continue

            try:
                element = self.driver.find_element_by_css_selector("button.company"
                                                                   "-profile__expand-button")
                self.driver.execute_script('arguments[0].click()', element)
            except exceptions.NoSuchElementException:
                self.logger.warning("DOWNLOADED NO DETAILED INFORMATION FOR %s",
                                    row['ticker_symbol'])
            except exceptions.WebDriverException:
                self._set_record_status(row=row,
                                        status='failed')
                continue

            try:

                self.save_file(content=self.driver.page_source, save_path=path_name,
                               do_threshold=True, row=row)

            except IOError:
                self.logger.exception("FAILED DURING SAVING TO FILE FOR %s", row['ticker_symbol'])
                self._set_record_status(row=row,
                                        status='failed')

        self.logger.info("ENDED DOWNLOADING COMPANIES INFO")
