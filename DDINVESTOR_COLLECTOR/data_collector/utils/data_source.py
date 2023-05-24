# import standard
import codecs
import glob
import json
import logging
import os
from enum import Enum
from pathlib import Path
from typing import AnyStr, Optional, Tuple
from functools import lru_cache
# import third-party
import pandas as pd


# import own

class ScrappingMode(Enum):
    history = 1
    current = 2


class FileTypes(Enum):
    csv = 1
    html = 2


class DataSource:
    SCRAPPING_MODE = None
    READ_TOKEN = False
    TOKEN = None

    @staticmethod
    def open_file(path: str) -> bytes:
        with codecs.open(path, 'r') as file:
            content = file.read()
        return content

    @staticmethod
    def save_file(save_path: str, content: AnyStr) -> None:
        """Generic function to save files"""
        with codecs.open(save_path, 'w') as file:
            file.write(content)

    @classmethod
    def generate_mode_path(cls, path: str) -> str:
        full_path = cls.get_access_to_environment()
        choosed = path % cls.SCRAPPING_MODE
        return os.path.join(full_path, choosed)

    @classmethod
    def set_scrapping_mode(cls, scrapping_mode: ScrappingMode = ScrappingMode.history) -> None:
        cls.SCRAPPING_MODE = scrapping_mode.name

    @classmethod
    def get_access_to_environment(cls, env_name: str = 'DDINVESTOR') -> Optional[str]:
        env_var = os.environ.get(env_name)
        if env_var is None:
            raise EnvironmentError(f'THERE IS NO {env_name} IN ENVIRONMENT ')
        else:
            return env_var

    @classmethod
    def create_logger(cls, name: str, file_name: str, data_phase: str = 'data_collector') \
            -> logging.Logger:
        path = cls.get_access_to_environment()
        logger_files = f'{path}/{data_phase}/loggers'
        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
        path = os.path.join(logger_files, f'{file_name}.log')
        file_handler = logging.FileHandler(path)
        file_handler.setLevel(logging.WARNING)
        file_handler.setFormatter(formatter)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        logger.addHandler(file_handler)
        return logger

    @staticmethod
    def df_to_tuples(frame: pd.DataFrame) -> [tuple()]:
        return [tuple(x) for x in frame.to_numpy()]

    @staticmethod
    def get_newest_folder(path: str) -> str:
        _data_path = DataSource.get_access_to_environment()
        path_to_look = os.path.join(_data_path, path)
        list_files = glob.glob(f"{path_to_look}/*_*_*")
        newest_folder = max(list_files, key=os.path.getmtime)
        return newest_folder

    @classmethod
    def read_session_token(cls) -> str:
        mod_path = Path(__file__).resolve().parent.parent
        configs_path = mod_path.joinpath('./configs/config.json')
        with open(configs_path) as file:
            token = json.load(file)
        cls.READ_TOKEN = True
        cls.TOKEN = token['token']

    @classmethod
    def get_session_token(cls) -> str:
        if cls.READ_TOKEN:
            return cls.TOKEN
        else:
            cls.read_session_token()
            return cls.TOKEN

    @staticmethod
    def create_folder(path: str) -> bool:
        """Creates a folder and returns a bool indicating if directory exists or not"""
        try:
            os.mkdir(path)
            return True
        except FileExistsError:
            return False

    @staticmethod
    def read_parquet_to_df(path: str = None) -> pd.DataFrame:
        """Read csv to pandas dataframe, change pandas Null values
        python Nulls"""
        check_file_extension = path[-13:]
        extension = '.parquet.gzip'
        if check_file_extension != extension:
            path +=extension

        raw_df = pd.read_parquet(path)
        clean_df = raw_df.where(pd.notnull(raw_df), None)
        return clean_df

    @staticmethod
    def write_df_to_parquet(path: str = None, content: pd.DataFrame = None) -> None:
        """Writes pandas dataframe to the given directory into parquet file."""
        check_file_extension = path[-13:]
        extension = '.parquet.gzip'
        if check_file_extension != extension:
            path +=extension

        content.to_parquet(compression='gzip', path=path, index=False)

    @staticmethod
    def write_df_to_csv(path: str = None, content: pd.DataFrame = None):
        check_file_extension = path[-4:]
        extension = '.csv'
        if check_file_extension !=extension :
            path = path + extension

        content.to_csv(path, index=False)
    @staticmethod
    def write_df_to_json(path: str = None, content: pd.DataFrame = None) -> None:
        """Writes pandas dataframe to the given directory into parquet file."""
        check_file_extension = path[-5:]
        if check_file_extension != '.json':
            path = path+".json"
        content.reset_index(drop=True,inplace=True)

        content.to_json(path, orient='records')

    @staticmethod
    def get_config_paths(parsing_path: str = 'DATA_LAKE') -> Tuple[str, str]:
        """Creates a folder parsed which stores all parsed datasets,
        And returns the tuple of strings which indicate where are the files to parse,
        and where to save parsed files"""
        scraped_path = DataSource.generate_mode_path(parsing_path)
        # delete creating newest parsed folder, and add creating parsed folder for the whole
        # subfeed
        newest_folder = DataSource.get_newest_folder(scraped_path)
        processed_date = os.path.basename(newest_folder)
        path_to_parsed_files = os.path.join(scraped_path, 'parsed')
        DataSource.create_folder(path_to_parsed_files)
        parsed_path = os.path.join(path_to_parsed_files,
                                   f'parsed_{processed_date}')
        return newest_folder, parsed_path
