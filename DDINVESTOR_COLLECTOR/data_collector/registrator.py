"""Starting point for any given feed"""
from utils import session_mode, ScrappingMode
from feeds import DayZeroOrchestrator


@session_mode(restart=False, scrapping_mode=ScrappingMode.history)
def main(config=None):
    """Main function to run feeds with specified configuration from
    json file"""
    meta_scraper = DayZeroOrchestrator()
    meta_scraper.run()



if __name__ == "__main__":
    CONFIG_DICT = {'scrape_comp_info': True,
                   'parse_comp_info': True,
                   'scrape_comp_news': True,
                   'parse_comp_news': True,
                   'scrape_history_news': False}
    main(config=CONFIG_DICT)
