"""Orchestrator for getting subfeed Business Insider"""
# import standard
# import third-party
# import own
from utils import ScrappingMode
from .orchestrator import Orchestrator
from parsers import BizInsiderNewsLinksParser, BizInsiderCompaniesInfoParser,BizInsiderNewsContentParser
from scrapers import BizInsiderNewsLinks, BizInsiderCompaniesInfo,BizInsiderNewsContent


class BizInsiderOrchestrator(Orchestrator):
    """Orchestrator for Business Insider domain which contains scrapper-parser operation"""

    def __init__(self):
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__, )

    def run(self, instructions=None):
        """Run method which specifies to options either Scrapping History:
        scrapper_comp_info Gets all of the data about the Companies and
        scrapper_news gets all the pages which have a list of news titles for each company
        OR Current mode in which
        scrapper_comp_info works the same as in history mode but
        scrapper_news gets only current pages
        """
        if instructions["scrapping_mode"] == ScrappingMode.history:
            self._run_defined_mode(do_scrape_current=False)
        elif instructions["scrapping_mode"] == ScrappingMode.current:
            self._run_defined_mode(do_scrape_current=True)

    def _run_defined_mode(self, do_scrape_current):
        parser_comp_info = BizInsiderCompaniesInfoParser()
        scrapper_comp_info = BizInsiderCompaniesInfo()
        scrapper_comp_info.download_companies_information()
        parser_comp_info.parse_files()
        scrapper_news = BizInsiderNewsLinks()
        scrapper_news.download_news(scrape_current=do_scrape_current)
        parser_news = BizInsiderNewsLinksParser()
        parser_news.parse_files()
        scrapper_news_text = BizInsiderNewsContent()
        scrapper_news_text.download_news_content()
        parser_news_text = BizInsiderNewsContentParser()
        parser_news_text.parse_files()
