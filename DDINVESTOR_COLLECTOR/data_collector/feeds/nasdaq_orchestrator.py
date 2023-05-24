"""Orchestrator for getting subfeed Nasdaq"""
# import standard

# import third party

# import own
from utils import ScrappingMode
from .orchestrator import Orchestrator
from scrapers import NASDAQNewsContent, NASDAQCompaniesInfo, NASDAQNewsLinks
from parsers import NASDAQNewsLinksParser, NasdaqCompanyDetailsParser, NASDAQNewsContentParser


class NasdaqOrchestrator(Orchestrator):
    """Orchestrator for Nasdaq domain which contains scrapper-parser operation"""

    def __init__(self):
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__, )

    def run(self, instructions=None):
        """Runs the method right now in two modes: Scrapping History:
        scrapper_news_links scrapes all the available list of links to the news on Nasdaq
        scrapper_news_text then scrappes all of them by using the earlier parsed list of links
        scrapper_comp_info scrapes today information about the company
        BUT in scrapping current:
        scrapper_news_links scrappes only the pages with current news (those which contain
        phrase hour or minute)
        scrapper_news_text scrapes the newes by using the parsed list from current
        and scrapper_comp_info works the same as in history"""

        if instructions["scrapping_mode"] == ScrappingMode.history:
            self._run_defined_mode(do_scrape_current=False)
        elif instructions["scrapping_mode"] == ScrappingMode.current:
            self._run_defined_mode(do_scrape_current=True)

    def _run_defined_mode(self, do_scrape_current: bool = True):
        scrapper_news_links = NASDAQNewsLinks()
        scrapper_news_links.scrape_news_links(scrape_current=do_scrape_current)
        parser_news_links = NASDAQNewsLinksParser()
        parser_news_links.parse_files()
        scrapper_news_text = NASDAQNewsContent()
        scrapper_news_text.scrape_news_content()
        parser_news_text = NASDAQNewsContentParser()
        parser_news_text.parse_files()
        scrapper_comp_info = NASDAQCompaniesInfo()
        scrapper_comp_info.scrape_companies_info()
        parser_comp_info = NasdaqCompanyDetailsParser()
        parser_comp_info.parse_files()
