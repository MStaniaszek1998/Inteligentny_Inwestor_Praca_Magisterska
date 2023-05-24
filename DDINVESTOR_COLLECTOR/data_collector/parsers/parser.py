"""Base class for each parser which gives the access to common use functions"""
import glob
import os
from abc import abstractmethod, ABC
from datetime import datetime
# import standard
from typing import Optional, Tuple, AnyStr, Dict, Union

# import third-party
from bs4 import BeautifulSoup
import bs4
# import own
from utils import DataSource, FileTypes


class Parser(ABC):
    """Every parser dervies from this class in order to get access to the standarized logger and
    common functions to check if the attribute is available in the root or soup"""

    def __init__(self, logger_name: str = None, logger_file_name: str = None,
                 path_to_scrapped: str = None):
        self.processed_base, self.processed_filename = "", " "
        self.processed_directory = ""
        self.config_path = path_to_scrapped
        self.logger = DataSource.create_logger(name=logger_name,
                                               file_name=logger_file_name)

    def _attach_ticker_to_path(self, ticker):
        ticker_file_name = f"{ticker}_{self.processed_filename}"
        joined_path_ticker = os.path.join(self.processed_base, ticker_file_name)
        return joined_path_ticker

    def find_in_soup(self, soup: BeautifulSoup = None, tag: str = None, attributes: Dict[str, str] =
    None, key: str = None, value_to_get: str = None, get_only_text: bool = True) -> Optional[str]:
        try:
            if get_only_text:
                element = soup.find(tag, attrs=attributes).text
                return element
            else:
                element = soup.find(tag, attrs=attributes).get(value_to_get)
                return element
        except:
            self.logger.info("NO ELEMENT FOR KEY %s", key)
            return None

    def _check_if_attr_exists(self, xpath: str, root, attribute: str,
                              return_all_join: bool = False) -> Optional[str]:
        """Checks if the attribute is available in the root of lxml parsed document"""
        try:
            main = root.xpath(xpath)

            if return_all_join:
                return "~".join(main)
            elif return_all_join == False:
                return main[0]
            else:
                return None
        except IndexError:
            self.logger.info("NO VALUE FOR %s", attribute)
            return None

    def get_next_file(self, file_type: FileTypes = FileTypes.html,
                      return_path: bool = False) -> Tuple[str, AnyStr]:
        """Generic function for unpacking and getting content for further scrapping.
        There is an exception for getting path to the to-be-scrapped file, because some scrappers
        needs the date of the file when it was scrapped"""
        newest_folder, parsed_path = DataSource.get_config_paths(parsing_path=self.config_path)
        file_to_process = f"{newest_folder}/*.{file_type.name}"
        self.processed_directory = parsed_path
        self.processed_base, self.processed_filename = os.path.split(parsed_path)
        for path in glob.glob(file_to_process):
            _, filename = os.path.split(path)
            ticker = filename.split('_')[0]
            if return_path:
                yield ticker, path
            else:
                content = DataSource.open_file(path=path)
                yield ticker, content

    def _prepare_file_to_scrape(self, path: str = None,
                                ticker: str = None) -> Dict[str, Union[AnyStr, datetime]]:
        file_metainfo = dict()
        file_metainfo['html_data'] = DataSource.open_file(path=path)
        file_metainfo['ticker'] = ticker
        day, month, year = "_".join(path.split("_")[-3:]).split(".")[0].split("_")
        file_metainfo['collected_date'] = datetime(year=int(year), month=int(month), day=int(day))
        return file_metainfo
