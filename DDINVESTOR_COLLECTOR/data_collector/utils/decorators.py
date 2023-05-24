# import standard
import datetime
import json
import os
import uuid
from pathlib import Path

# import third-party
import pandas as pd

from .data_source import DataSource, ScrappingMode
from .manager_pgsql import ManagerPgSQL


# import own


def session_mode(restart: bool = True, scrapping_mode: ScrappingMode = ScrappingMode.history):
    def decorator(func):
        def wrapper(*args, **kwargs):
            DataSource.set_scrapping_mode(scrapping_mode)
            if restart:
                token = str(uuid.uuid4())

                mod_path = Path(__file__).resolve().parent.parent
                configs_path = mod_path.joinpath('./configs/config.json')
                with open(configs_path, 'w') as file:
                    json.dump({'token': token}, file)
            return func(*args, **kwargs)

        return wrapper

    return decorator





def connection_to_postgresql(database_name: str = 'DDInvestor'):
    def decorator(func):
        def wrapper(*args, **kwargs):
            manager = ManagerPgSQL(database_name=database_name)

            with manager.get_connection() as conn:
                kwargs['conn'] = conn

                result = func(*args, **kwargs)
                return result

        return wrapper

    return decorator


@connection_to_postgresql(database_name='DDInvestorMetadata')
def get_metadata(conn=None, get_query: str = None) -> pd.DataFrame:
    company_list = pd.read_sql_query(get_query, conn)
    return company_list


def get_companies_to_scrape(func):
    def wrapper(*args, **kwargs):
        invoker = args[0]
        token = DataSource.get_session_token()
        query = invoker.get_query_companies_to_scrape(token=token)
        company_list = get_metadata(get_query=query)
        kwargs['company_list'] = company_list
        return func(*args, **kwargs)

    return wrapper


def make_daily_folder(path: str = "DataLake"):
    def decorator(func):
        def wrapper(*args, **kwargs):
            save_path = DataSource.generate_mode_path(path)
            now = datetime.datetime.now()
            DataSource.create_folder(save_path)
            path_first = os.path.join(save_path, f"{now.day}_{now.month}_{now.year}")
            DataSource.create_folder(path_first)

            kwargs['today_path'] = path_first
            result = func(*args, **kwargs)
            return result

        return wrapper

    return decorator
