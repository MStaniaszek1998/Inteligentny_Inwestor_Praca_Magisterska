"""Special meta-feed
Instanties the meta data collection for all scrappers.
It creates a dataflow to get all necessary information for further scrapping"""
import pandas as pd
from parsers import BizInsiderIndexesParser, BizInsiderCompaniesInfoParser, \
    MultiSourceTwitterUrlsParser
from scrapers import BizInsiderIndexes, BizInsiderCompaniesInfo, MultiSourceTwitterUrls
from utils import DataSource, connection_to_postgresql
from .orchestrator import Orchestrator


class DayZeroOrchestrator(Orchestrator):
    """Special Class which collects all needed meta_Data:
    This feed will only be used at day 0 of the production process.
    """

    def __init__(self):
        self.comp_list_parser = None
        self.detail_info_parser = None
        self.preprocess_webpage = None
        self.logger = DataSource.create_logger(name=__name__,
                                               file_name=self.__class__.__name__)

    def run(self, instructions=None):
        """Orechstrates the scrapping of all necessary metadata for data-scrappers.
        1. Downloads initial scrapping list of all 500 companies from Sp500
        2. Inserts it into the database
        3. Inserts into database urls, which are generated using a specific template per sub-feed
        4. Downloads the details about each company and updates the url column for the source
        called MainWebPage of each company.
        5. Scrapes the content of every company's webpage
        6. Parses the webpage and looks for the twitter url and saves it.
        7. Inserts the twitter account url for each company"""

        # Get initial scrapping list of companies
        self.logger.info("STARTING METADATA FLOW")
        self.logger.info("GETTING LIST OF SP500")
        comp_list_scraper = BizInsiderIndexes()
        self.comp_list_parser = BizInsiderIndexesParser()

        self.detail_info_parser = BizInsiderCompaniesInfoParser()
        self.preprocess_webpage = MultiSourceTwitterUrlsParser()

        comp_list_scraper.download_list()
        self.comp_list_parser.parse_files()
        path_to_list = self.comp_list_parser.processed_directory
        company_list = DataSource.read_parquet_to_df(path=path_to_list)
        self.logger.info("INSERTING LIST TO DATABASE")
        self.insert_meta_list_to_db(comp_list=company_list)
        # # Register all url's of scrapers
        self.logger.info("REGISTRATING ALL LINKS FOR SCRAPERS")
        self.register_scrappers(comps_list=company_list)
        # # Get all Details about the company including the company's website url
        self.logger.warning("GETTING URLS TO MAIN WEB PAGES FOR COMPANIES")
        detail_info_scraper = BizInsiderCompaniesInfo()
        detail_info_scraper.download_companies_information()
        self.detail_info_parser.parse_files()
        company_details = DataSource.read_parquet_to_df(
            path=self.detail_info_parser.processed_directory)
        self.logger.info("UPDATING COMPANIES WITH WEB PAGES")
        self.update_meta_list_urls(comp_list=company_details)
        # # Scrape main web pages of companies
        self.logger.info("GETTING MAIN WEB PAGES OF COMPANIES")
        twitter = MultiSourceTwitterUrls()
        twitter.get_companies_twitter_url()
        # Search for twitter urls on scraped main web pages
        self.preprocess_webpage.parse_files()
        twitter_urls = DataSource.read_parquet_to_df(path=
                                                     self.preprocess_webpage.processed_directory)
        self.update_twitter_urls(twitter_list=twitter_urls)
        self.logger.info("ENDED METADATA FLOW")

    @connection_to_postgresql(database_name="DDInvestorMetadata")
    def update_twitter_urls(self, twitter_list: pd.DataFrame = None, conn=None) -> None:
        cursor = conn.cursor()
        query_to_update = self.preprocess_webpage.to_update_sql
        for _, row in twitter_list.iterrows():
            try:
                cursor.execute(query_to_update, (row['twitter_url'], row['ticker']))
            except Exception as e:
                self.logger.exception("FAILED UPDATING TWITTER URLS")
        cursor.close()

    @connection_to_postgresql(database_name="DDInvestorMetadata")
    def update_meta_list_urls(self, comp_list: pd.DataFrame = None, conn=None) -> None:
        """Updates the url for the source MainWebsite, which is needed for scrapping twitter urls"""
        cursor = conn.cursor()
        query_update = self.detail_info_parser.to_update_sql
        for _, row in comp_list.iterrows():
            try:
                cursor.execute(query_update, (row['website'], row['ticker']))
            except Exception as e:
                self.logger.exception("FAILED UPDATING WEB PAGES URLS")
        cursor.close()

    @connection_to_postgresql(database_name="DDInvestorMetadata")
    def insert_meta_list_to_db(self, comp_list: pd.DataFrame = None, conn=None) -> None:
        """Inserts or Updates the companies information in the database for further scrappers"""
        cursor = conn.cursor()
        query_insert = self.comp_list_parser.to_insert_sql_query
        for _, row in comp_list.iterrows():
            cursor.execute(query_insert, (row['ticker'], row['name']))
        cursor.close()

    @connection_to_postgresql(database_name="DDInvestorMetadata")
    def register_scrappers(self, comps_list: pd.DataFrame = None, conn=None) -> None:
        """Registers all templates of links for the each scraper into the database.
        So they can start scrapping and use the tokenization system"""
        registrator = {
            'BizInsiderCompaniesInfo':
                'https://markets.businessinsider.com/stocks/{0}/company-profile',
            'BizInsiderNews': 'https://markets.businessinsider.com/news/{0}?p=',
            'Twitter': None,
            'NASDAQCompaniesInfo': 'https://www.nasdaq.com/market-activity/stocks/{0}',
            'YahooCompaniesHistoryPrice':
                'https://query1.finance.yahoo.com/v7/finance/download/{0}?',
            'YahooCompaniesInfo': "https://finance.yahoo.com/quote/{0}/profile?p={0}",
            "YahooConversations": "https://finance.yahoo.com/quote/{0}/community?p={0}",
            "NasdaqNewsLinks": "https://www.nasdaq.com/market-activity/stocks/{0}/news-headlines"
        }
        insert_scraper = """
        INSERT INTO CompaniesStatus(ticker_symbol,name,subfeed,Url)
        VALUES (%s,%s,%s,%s)
        """
        cursor = conn.cursor()
        for _, row in comps_list.iterrows():
            for key in registrator:
                if key == 'Twitter':
                    company_url = None
                else:
                    company_url = registrator[key].format(row['ticker'])
                values = (row['ticker'], row['name'], key, company_url)
                cursor.execute(insert_scraper, values)
        cursor.close()
