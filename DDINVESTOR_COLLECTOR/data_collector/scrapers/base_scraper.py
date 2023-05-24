"""Base Class for all scrapers.
Ensures the connection between data_collector and selenium one docker image using ip address.
It includes helper methods for each scraper i.e. maintains the session between scraper and
selenium, and gives access to generic functions for each scrapper"""
# import standard
import os
# import third-party
import random
import sys
import time
from datetime import datetime
from typing import Union, AnyStr

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.common.keys import Keys

# import own
from utils import DataSource, connection_to_postgresql


class BaseScraper:
    """BaseScraper
    All scrapers derived from this class.
    It gives the scrappers the acces to the: selenium's webdriver and maintins its session;
    tokenization system responsible for fault-tolerant scrapping; common JavaScript actions for
    each scrapper."""

    def __init__(self, logger_name: str, logger_file_name: str, use_selenium: bool = True,
                 source_name: str = __name__, threshold_value: int = None):
        print("Starting Scrapping")
        self.source_name: str = source_name
        self._use_selenium = use_selenium
        self.threshold_value: int = threshold_value
        if use_selenium:
            self.driver = webdriver.Remote('http://172.17.0.1:4444/wd/hub',
                                           DesiredCapabilities.CHROME)

        self.logger = DataSource.create_logger(logger_name, logger_file_name)

    def create_news_template_name(self, news_url: str = None, now: datetime = None) -> str:
        """Creates a template name in which the first part is date and later is the title with
        underscores"""
        cleaned_url = news_url[news_url.rfind('/') + 1:].replace('-', "_")

        name = f"{now.year}_{now.month}_{now.day}_{cleaned_url}"
        return name

    def save_file(self, save_path: str, content: AnyStr, do_threshold: bool = False,
                  row: pd.Series = None) -> None:
        """Special function with threshold value to save the content if the size is above the
        threshold. Otherwise do not save the content and mark scrapped file as failed status at
        tokenization system"""
        if do_threshold:
            content_size = sys.getsizeof(content)
            if content_size > self.threshold_value:
                DataSource.save_file(save_path=save_path, content=content)
                self._set_record_status(row=row, status='success')
            else:
                self._set_record_status(row=row, status='failed')
        else:
            DataSource.save_file(save_path=save_path, content=content)
            self._set_record_status(row=row, status='success')

    def save_file_without_token(self, save_path: str = None, content: AnyStr = None,
                                do_threshold: bool = False):
        """Case when there is a lot of pages to scrape but they are not listed in the database,
        because the number of pages can change i.e. amount of news, tweets etc."""
        if do_threshold:
            content_size = sys.getsizeof(content)
            if content_size > self.threshold_value:
                DataSource.save_file(save_path=save_path, content=content)
        else:
            DataSource.save_file(save_path=save_path, content=content)


    @staticmethod
    def create_ticker_path(path: str = 'DDInvestor', ticker: str = None, now: datetime = None,
                           file_ext: str = 'html') -> str:
        """Function which generate a path from a datetime and format template. Every scrapper
        uses it to save the content of the scrapped website into the standardized format"""
        path = os.path.join(path,
                            f"{ticker}_{now.day}_{now.month}_{now.year}.{file_ext}")
        return path

    def scroll_to_the_end_html(self) -> None:
        """Webdriver is scrolling to the end of the webpage and renders the content for the
        further scrapping."""
        html = self.driver.find_element_by_tag_name('html')
        html.send_keys(Keys.END)

    def renew_session(self) -> None:
        """If the error occurs the session with the docker selenium is renewed in order to avoid
        further errors from the website"""
        self.driver.quit()
        self.driver = webdriver.Remote('http://172.17.0.1:4444/wd/hub', DesiredCapabilities.CHROME)

    def __del__(self):
        """When the destructor is called it ends the session if its open"""
        if self._use_selenium:
            print("Ending Scrapping")
            self.driver.quit()

    @staticmethod
    def _sleep_randomize_duration(duration: int = 1, variance: float = 0.5) -> None:
        """Calculates the exact time how long to sleep.
        So having the duration equal to 1 and variance 0.5, it chooses the random number from 50
        to 150, and later divides by 100 to maintain the reasonable time.
        """
        low = (duration - variance) * 100
        high = (duration + variance) * 100
        return time.sleep(random.randint(low, high) / 100)

    @classmethod
    def sleep(cls, duration: int = 1, variance: Union[bool, float] = False) -> None:
        """Specifies how long the scrapping session has to wait for the further scrapping in
        order to avoid getting banned"""
        if not variance:
            time.sleep(duration)
        else:
            cls._sleep_randomize_duration(duration, variance)

    def acquire_yahoo_access(self) -> None:
        """Special function for main Yahoo feed, which goes to the main yahoo website and accepts
        the terms. It is done in order to acquire a guccounter, which enables scrapers further
        parsing. Guccounter is considered to be a redirect virus."""
        self.driver.get("https://finance.yahoo.com/")
        time.sleep(random.randint(1, 3))
        self.driver.find_element_by_name('agree').click()

    # Token System
    def get_query_companies_to_scrape(self, token=None):
        """Creates the query for the tokenization system for given class.
        It returns all not completed rows during scrapping, which were not processed on the ongoing
        session. The session is marked by using the uuid4 token.
        It ensures a fault-tolerancy in the system. So that if the scraper fails to scrape the
        url for the given company, it is annoted in the database and within the same session it
        scrapes all not collected eariler urls."""
        query = f"""
                SELECT ticker_symbol,name,url 
                FROM CompaniesStatus
                WHERE subfeed = '{self.source_name}' 
                and (not
                        (token = '{token}' and status='success')
                        or token is NULL)"""

        return query

    @connection_to_postgresql(database_name='DDInvestorMetadata')
    def _set_token_status(self, conn=None, row: pd.Series = None) -> None:
        """Marks the url with the token session, so that scraper has tried to scrape this url"""
        class_name = self.source_name
        cursor = conn.cursor()
        token = DataSource.get_session_token()
        query = """
        UPDATE CompaniesStatus 
        SET token = %s 
        WHERE subfeed=%s and Url=%s
        """
        cursor.execute(query, (token, class_name, row['url']))
        cursor.close()

    @connection_to_postgresql(database_name='DDInvestorMetadata')
    def _set_record_status(self, conn=None, row: pd.Series = None, status: str = 'failed') -> None:
        """Sets the status for the url of the company if the given scraper fails or completes
        scrapping the web page. """
        cursor = conn.cursor()
        class_name = self.source_name
        token = DataSource.get_session_token()
        query = """
        UPDATE CompaniesStatus 
        SET status = %s 
        WHERE subfeed=%s AND Url=%s AND token=%s 
        """
        cursor.execute(query, (status, class_name, row['url'], token))
        cursor.close()
