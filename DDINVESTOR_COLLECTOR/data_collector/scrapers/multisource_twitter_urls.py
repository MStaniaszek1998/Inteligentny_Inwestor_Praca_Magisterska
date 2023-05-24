"""Scraper to search for twitter urls in each company's home page. Later it saves it to the
SQL database"""
import pandas as pd
import selenium
from datetime import datetime
# import own
from utils import make_daily_folder, get_companies_to_scrape, DataSource
from .base_scraper import BaseScraper


class MultiSourceTwitterUrls(BaseScraper):
    """metadata_scraper
    MultisourceTwitterUrls doesnt have a specified name convention so:
    source_name_thing_to_get. It's because its not restricted to only one source. """

    def __init__(self):
        self.source_name = 'MainWebsite'
        self.threshold_value = 5673
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         source_name=self.source_name, threshold_value=self.threshold_value)
        self.to_update_sql = """
        UPDATE CompaniesStatus 
        SET Url = ? 
        WHERE Source='Twitter' and ticker_symbol=? 
        """

    @make_daily_folder(path='data_collector/multisource/%s/MultiSource_TwitterUrls')
    @get_companies_to_scrape
    def get_companies_twitter_url(self, today_path: str = None,
                                  company_list: pd.DataFrame = None) -> None:
        """Searches for possible twitter url account of the company"""
        df_companies = company_list
        self.logger.info("STARTED SCRAPPING MULTI SOURCE")

        df_companies = df_companies.sort_values(by=['ticker_symbol'])

        counter = 0

        bad = 0
        for _, row in df_companies.iterrows():
            now = datetime.now()
            self._set_token_status(row=row)
            counter += 1
            url = row['url']
            self.logger.info("SCRAPPING WEBSITE FOR %s", row['ticker_symbol'])
            try:
                self.driver.get(url=url)
                self.scroll_to_the_end_html()

                path = BaseScraper.create_ticker_path(path=today_path,
                                                      ticker=row['ticker_symbol'],
                                                      now=now)

                self.save_file(content=self.driver.page_source, save_path=path,
                               do_threshold=True, row=row)

            except selenium.common.exceptions.NoSuchElementException:
                print("Page not found")
                self._set_record_status(row=row,
                                        status='failed')
                bad += 1
                self.renew_session()

                continue
            except selenium.common.exceptions.WebDriverException:
                self._set_record_status(row=row,
                                        status='failed')
                continue
            except Exception as e:
                self.logger.exception("SCRAPPER ERROR")
                self._set_record_status(row=row,
                                        status='failed')
                continue
        self.logger.info("ENDED SCRAPPING MULTI SOURCE")
