# import standard

# import third-party
import psycopg2

# import own
from .data_source import DataSource


class ManagerPgSQL:
    ARE_TABLES_DONE = False

    def __init__(self, database_name='DDInvestorMetadata'):
        self.connection = f"user=postgres password=postgres host=172.17.0.1 port=5432 " \
                          f"dbname={database_name} "

        if database_name == 'DDInvestorMetadata':
            self.Create_metatables()
        else:
            pass

    def get_connection(self):

        return psycopg2.connect(self.connection)

    @classmethod
    def _create_tables_execute(cls, table_name: str, query: str, cursor) -> None:
        try:

            cls.ARE_TABLES_DONE = True
            cursor.execute(query)

        except psycopg2.errors.DuplicateTable:
            pass
            # self.logger.debug(f"ERROR CREATING TABLE {table_name} - Already Exist")
        cursor.close()

    def Create_metatables(self) -> None:
        if not self.ARE_TABLES_DONE:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                create_companiesstatus_table = """
                CREATE TABLE CompaniesStatus(
                        ticker_symbol varchar(10) not null,
                        name varchar(100) not null,
                        token UUID,
                        status varchar(100),
                        subfeed varchar(200) not null,
                        url varchar(250),
                        PRIMARY KEY (ticker_symbol,subfeed))
                """
                self._create_tables_execute(cursor=cursor, query=create_companiesstatus_table,
                                            table_name='CompaniesStatus')
