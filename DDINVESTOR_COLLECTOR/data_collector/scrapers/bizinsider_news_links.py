# import standard
"""Sub-feed of the main feed - Business Insider,
It scrapes either all of the headline news or just current (only one page) for each company
from the source"""
import datetime
# import own
from typing import Tuple

import pandas as pd
# import third-party
from tqdm import tqdm

from utils import make_daily_folder, DataSource, get_companies_to_scrape
from .base_scraper import BaseScraper


class BizInsiderNewsLinks(BaseScraper):
    """Scrapes the headline content,
    the date of the news,
    the news service which wrote that news
    and also the url to the details of the news."""

    def __init__(self):
        self.source_name = 'BizInsidersNewsLinks'
        self.threshold_value = 154970
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         source_name=self.source_name, threshold_value=self.threshold_value)


    @staticmethod
    def _check_valid_max_page(page: str) -> bool:
        """Checks if the given string is a digit"""
        checker = any(map(str.isdigit, page))
        return checker

    def detect_max_sites(self, ) -> Tuple[int, bool]:
        """It searches for the maximum number on the slider on website, to find out how many
        subpages there are to scrape. """
        self.scroll_to_the_end_html()

        sites = self.driver.find_elements_by_xpath("//a[contains(@href,'?p=')]")
        all_sites = [x.text for x in sites]
        if len(all_sites) == 0:
            return 2
        max_site = sites[-1].text
        if max_site == '':
            return sites[-2].text
        else:
            return max_site

    @make_daily_folder(path='data_collector/business_insider/%s/BusinessInsider_News')
    @get_companies_to_scrape
    def download_news(self, today_path: str = None, company_list: pd.DataFrame = None,
                      scrape_current: bool = False) -> None:
        """Gets the list of companies to scrape from the tokenization system.
        Saves the scraped content for the coresponding daily folder given from today_path"""
        self.logger.info('STARTING DOWNLOADING RAW NEWS')

        # temporary filter downloading
        filtered_companies = company_list.loc[company_list['ticker_symbol'].isin(['nvda', 'twtr',
                                                                              'vz'])]

        for _, row in tqdm(filtered_companies.iterrows()):
            now = datetime.datetime.now()
            self._set_token_status(row=row)
            self.logger.info("DOWNLOADING NEWS FOR %s", row['ticker_symbol'])
            url = row['url']
            self.driver.get(url)
            num_sites = self.detect_max_sites()

            for page in range(1, int(num_sites)):
                save_page = True
                try:

                    self.driver.get(url + str(page))
                    self.scroll_to_the_end_html()
                    BaseScraper.sleep(1, .5)
                    if scrape_current:
                        has_current_news = self._contains_current_news(row=row)

                        if has_current_news == False:
                            save_page = False

                except:
                    self.logger.exception("SCRAPPER ERROR:")
                    self._set_record_status(row=row,
                                            status='failed')

                try:
                    if save_page:
                        page -= 1
                        path = BaseScraper.create_ticker_path(path=today_path,
                                                              ticker=f"{row['ticker_symbol']}_{page}",
                                                              now=now)

                        self.save_file_without_token(content=self.driver.page_source,
                                                     save_path=path,
                                                     do_threshold=True)
                    else:
                        break
                except IOError:

                    self._set_record_status(row=row,
                                            status='failed')

                    self.logger.exception("FAILED WRITTING FILE")

        self.logger.info('ENDED DOWNLOADING RAW NEWS')

    def _contains_current_news(self, row):
        pharses = ['h', 'm', 's']
        published_dates = self.driver.find_elements_by_xpath(
            "//span[@class='warmGrey source-and-publishdate']")
        found_dates = [date.text.split(" ")[-1] for date in published_dates]
        joined_dates = " ".join(found_dates).lower()
        if any(ext in joined_dates for ext in pharses):
            return True
        else:
            return False
