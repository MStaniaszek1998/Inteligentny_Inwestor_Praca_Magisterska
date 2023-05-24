"""Special scraper for scrapping links of news.
It's a meta scraper which is only used to get all links for news which will be later scrapped
by another normal data scraper"""
# import standard
from datetime import datetime
# import third party
from typing import Optional

import pandas as pd
# import own
from selenium.webdriver.common.keys import Keys
from tqdm import tqdm

from scrapers.base_scraper import BaseScraper
from utils import get_companies_to_scrape, make_daily_folder


class NASDAQNewsLinks(BaseScraper):
    """Scraper all the links of the newses and saves the downloaded html into the data lake
    under nasdaq subfeed"""

    def __init__(self):
        self.source_name = 'NasdaqNewsLinks'
        self.threshold_value = 252563
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         source_name=self.source_name, threshold_value=self.threshold_value)

    def _check_if_has_current_news(self, row) -> Optional[bool]:
        phrases = ['hour', 'minute', 'second']
        try:
            published_dates = self.driver.find_elements_by_xpath(
                "//span[@class='quote-news-headlines__date']")
            found_dates = [date.text for date in published_dates]
        except:
            self.logger.exception("ERROR %s", row['ticker_symbol'])
            self._set_record_status(row=row,
                                    status='failed')
            return None
            # None means that there was a problem with getting stale elements i.e. date times
            # from page

        joined_dates = " ".join(found_dates).lower()
        if not any(ext in joined_dates for ext in phrases):
            return False
        else:
            return True

    @make_daily_folder(path="data_collector/nasdaq/%s/Nasdaq_NewsLinks")
    @get_companies_to_scrape
    def scrape_news_links(self, today_path: str, company_list: pd.DataFrame,
                          scrape_current: bool = True) -> None:
        """Gets all the links for a company for a nasdaq subfeed.
        It goes to the page given from the database. Goes through the pagination pages on the
        webpage and saves each list of newses to the Data lake. Later those urls will be used
        in scrapping of news"""
        self.logger.info("STARTING SCRAPPING LINKS TO THE NEWS")
        filtered_companies = company_list.loc[
            company_list['ticker_symbol'].isin(['nvda', 'twtr', 'vz'])]
        for _, row in tqdm(filtered_companies.iterrows(), total=filtered_companies.shape[0]):

            self.logger.info("GETTING NEWS FOR %s", row['ticker_symbol'])
            now = datetime.now()
            self._set_token_status(row=row)
            url = row['url']
            path_name = BaseScraper.create_ticker_path(path=today_path,
                                                       ticker=f"{row['ticker_symbol']}_%s",
                                                       now=now)
            try:
                BaseScraper.sleep(duration=1, variance=0.5)
                self.driver.get(url)
                html = self.driver.find_element_by_tag_name('html')
                html.send_keys(Keys.END)
            except:
                self.logger.exception("ERROR WHILE GETTING LINK FOR %s", row['ticker_symbol'])
                self._set_record_status(row=row,
                                        status='failed')
                continue
            status = True
            pages = 0
            try:
                datestamps_error = False
                while status:
                    BaseScraper.sleep(duration=1, variance=0.5)
                    path = path_name % str(pages)

                    if scrape_current == True:

                        scrape_next_page = self._check_if_has_current_news(row=row)
                        if scrape_next_page is None:
                            datestamps_error = True
                            break
                        if scrape_next_page == False:
                            break
                        else:
                            self.save_file_without_token(save_path=path,
                                                         content=self.driver.page_source,
                                                         do_threshold=True)
                    else:
                        self.save_file_without_token(save_path=path,
                                                     content=self.driver.page_source,
                                                     do_threshold=True)
                    stat_check = self.driver.execute_script('return document.querySelector("body > '
                                                            'div.dialog-off-canvas-main-canvas > '
                                                            'div '
                                                            '> main > div > '
                                                            'div.quote-subdetail__content.quote'
                                                            '-subdetail__content--new > '
                                                            'div:nth-child(2) > div > '
                                                            'div.quote-subdetail__indented'
                                                            '-components '
                                                            '> div > '
                                                            'div.quote-news-headlines.quote-news'
                                                            '-headlines--paginated > div > div > '
                                                            'button.pagination__next").getAttribute('
                                                            '"disabled")')

                    if stat_check == 'true':
                        status = False

                    else:
                        BaseScraper.sleep(duration=4, variance=0.5)
                        self.driver.execute_script('document.querySelector("body > '
                                                   'div.dialog-off-canvas-main-canvas > div > main > '
                                                   'div > '
                                                   'div.quote-subdetail__content.quote'
                                                   '-subdetail__content--new > div:nth-child(2) > div '
                                                   '> div.quote-subdetail__indented-components > div '
                                                   '> '
                                                   'div.quote-news-headlines.quote-news-headlines'
                                                   '--paginated > div > div > '
                                                   'button.pagination__next").click()')
                        pages += 1
                if not datestamps_error:
                    self._set_record_status(row=row, status='success')
            except:
                self._set_record_status(row=row,
                                        status='failed')
