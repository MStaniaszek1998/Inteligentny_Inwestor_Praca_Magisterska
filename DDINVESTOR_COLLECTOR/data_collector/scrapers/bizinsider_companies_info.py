"""Sub-feed of the main feed - Business Insider
It collects the detail information about each company"""
# import standard
import datetime

import pandas as pd
# import third-party
from tqdm import tqdm

from utils import make_daily_folder, DataSource, get_companies_to_scrape
# import own
from .base_scraper import BaseScraper


class BizInsiderCompaniesInfo(BaseScraper):
    """data_scraper
    Gets the detail information about each company like website, description and location"""

    def __init__(self):
        self.source_name = 'BizInsiderCompaniesInfo'
        self.threshold_value = 6354
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         source_name=self.source_name,threshold_value=self.threshold_value)

    @make_daily_folder(path='data_collector/business_insider/%s/BusinessInsider_CompanyDetails')
    @get_companies_to_scrape
    def download_companies_information(self, today_path: str = None,
                                       company_list: pd.DataFrame = None) -> None:
        """Gets list of companies from the tokenization system to scrape and the
        earlier defined path to daily folder"""
        self.logger.info("STARTING DOWNLOADING RAW COMPANY INFORMATION")



        for _, row in tqdm(company_list.iterrows(), total=company_list.shape[0]):
            now = datetime.datetime.now()
            ticker = row['ticker_symbol']
            self._set_token_status(row=row)
            url = row['url']
            self.logger.info("GETTING INFO FOR %s", row['ticker_symbol'])
            self.driver.get(url)
            self.scroll_to_the_end_html()
            BaseScraper.sleep(1, 0.5)

            path_name = BaseScraper.create_ticker_path(path=today_path,
                                                       ticker=ticker, now=now)
            div_to_get = '//*[@id="site"]/div/div[3]/div[3]/div/div[1]'
            content = self.driver.find_element_by_xpath(div_to_get).get_attribute('innerHTML')
            try:
                self.save_file(content=content, save_path=path_name,do_threshold=True, row=row)

            except IOError:

                self.logger.exception("FAILED DURING SAVING FOR %s", row['ticker_symbol'])

                self._set_record_status(row=row,
                                        status='failed')

        self.logger.info("ENDED DOWNLOADING RAW COMPANY INFORMATION")
