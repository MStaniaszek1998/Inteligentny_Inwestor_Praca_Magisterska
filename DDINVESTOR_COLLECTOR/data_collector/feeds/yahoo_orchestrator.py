"""Orchestrator for subfeed  Yahoo which leads scrapper-parser operation"""
from utils import ScrappingMode
from .orchestrator import Orchestrator
from parsers import YahooPriceParser, YahooCompaniesInfoParser
from scrapers import YahooCompaniesInfo, YahooCompaniesPrice


class YahooOrchestrator(Orchestrator):
    """Orchestrator for Yahoo domain which contains scrapper-parser operation"""

    def __init__(self):
        super().__init__(logger_name=__name__, logger_file_name=self.__class__.__name__, )

    def run(self, instructions=None) -> None:
        """Run method which specifies to options either Scrapping History:
        scrapper_price collects all of the data starting from -10 years until now
        scrapper_comp_info gets today data about each company
        BUT in scrapping Current
        scrapper_price gets data only for today
        and scrapper_comp_info works the same
        """
        if instructions["scrapping_mode"] == ScrappingMode.history:
            self._run_defined_mode(do_scrape_history=False)
        elif instructions["scrapping_mode"] == ScrappingMode.current:
            self._run_defined_mode(do_scrape_history=True)

    def _run_defined_mode(self, do_scrape_history: bool = None, years_behind: int = 10) -> None:
        scrapper_comp_info = YahooCompaniesInfo()
        scrapper_comp_info.scrape_companies_info()
        parser_comp_info = YahooCompaniesInfoParser()
        parser_comp_info.parse_files()
        scrapper_price = YahooCompaniesPrice()
        scrapper_price.download_price(do_scrape_history=do_scrape_history,
                                           years_behind=years_behind)
        parser_price = YahooPriceParser()
        parser_price.parse_files()
