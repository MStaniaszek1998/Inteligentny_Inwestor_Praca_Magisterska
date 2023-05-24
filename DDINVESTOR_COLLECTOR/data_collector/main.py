"""Starting point for any given feed"""
from parsers import YahooCompaniesInfoParser, NASDAQNewsContentParser, BizInsiderNewsLinksParser, \
    NASDAQNewsContentParser, BizInsiderNewsContentParser, BizInsiderNewsLinksParser
from scrapers import BizInsiderNewsContent, YahooCompaniesPrice, BizInsiderNewsLinks,\
    NASDAQNewsLinks, NASDAQNewsContent,BizInsiderCompaniesInfo
from utils import session_mode, ScrappingMode
from parsers import NASDAQNewsContentParser, NASDAQNewsLinksParser, YahooCompaniesInfoParser,\
    YahooPriceParser, BizInsiderNewsLinksParser,NasdaqCompanyDetailsParser,\
    BizInsiderCompaniesInfoParser,YahooCompaniesInfoParser,YahooPriceParser, NasdaqCompanyDetailsParser


@session_mode(restart=True, scrapping_mode=ScrappingMode.history)
def main():
    """Main function to run feeds with specified configuration from
    json file"""
    # nasdaq_news_link = NASDAQNewsLinks()
    # nasdaq_news_link.scrape_news_links(scrape_current=True)
    # parser = NASDAQNewsLinksParser()
    # parser.parse_files()
    # # get news
    # news_nasdaq = NASDAQNewsContent()
    # news_nasdaq.scrape_news_content()
    # parser = NASDAQNewsContentParser()
    # parser.parse_files()
    #
    # buissness = BizInsiderCompaniesInfo()
    # buissness.download_companies_information()
    parser = BizInsiderCompaniesInfoParser()
    parser.parse_files()


if __name__ == "__main__":
    main()
