"""Sub-feed of the main feed Business Insider
It collects the metadata - list of companies to scrape and saves the raw html to folder"""
# import standard
import datetime
import os

from utils import make_daily_folder, DataSource
# import own
from .base_scraper import BaseScraper


# import third party


class BizInsiderIndexes(BaseScraper):
    """metadata-scraper
    Special initial scraper which collects all of the companies for further scrapping.
    It collects company's name and ticker symbol (unique symbol for the company at the market)"""
    def __init__(self):
        self.source_name = 'BizInsiderIndexes'
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__,
                         source_name=self.source_name)
        self.url: str = 'https://markets.businessinsider.com/index/components/s&p_500?p='

    @make_daily_folder(path='data_collector/business_insider/%s/Businessinsider_CompanyList')
    def download_list(self, today_path: str) -> None:
        """Gets only the today path which indicates where to put scrapped files"""
        self.logger.info("STARTING DOWNLOADING RAW COMPANIES LIST FOR SP500")



        for page_index in range(1, 11):
            now = datetime.datetime.now()
            BaseScraper.sleep(1, .5)
            url = self.url + str(page_index)
            try:
                self.driver.get(url)
                self.scroll_to_the_end_html()
            except Exception as e:
                self.logger.exception("SCRAPPER ERROR: ")

            path = os.path.join(today_path,
                                f'bi_{str(page_index)}_page_{now.day}_{now.month}_{now.year}.html')

            DataSource.save_file(content=self.driver.page_source,
                                 save_path=path)
            self.logger.info("DOWNLOADED PAGE %s", page_index)
        self.logger.info("ENDED DOWNLOADING RAW SOURCE")
