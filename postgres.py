from __future__ import annotations

import os
import json
import datetime
from typing import Union, List, Dict, Any, Optional, Callable

import psycopg2
import psycopg2.extensions
import psycopg2.extras
import psycopg2.errors

try:
    import dotenv
    dotenv.load_dotenv()
except ModuleNotFoundError:
    pass


def get_postgres_from_env() -> Postgres:
    """
    Returns a Postgres object with credentials from environment variables.

    If environment variables are not set, it uses default values.

    Returns:
        Postgres: A Postgres object with connection details.
    """
    return Postgres(
        host=os.environ.get('POSTGRES_HOST', 'localhost'),
        port=os.environ.get('POSTGRES_PORT', '5432'),
        user=os.environ.get('POSTGRES_USER', 'daniel'),
        password=os.environ.get('POSTGRES_PASSWORD', 'MyPassword123'),
        database=os.environ.get('POSTGRES_DATABASE', 'my_db')
    )


def sort(val):
    """
    Assigns a sorting priority to PostgreSQL data types.

    Args:
        val (str): A PostgreSQL data type.

    Returns:
        int: The sorting priority of the data type.
    """
    _sort = {'text': 0, 'real': 1, 'bigint': 2, 'integer': 3, 'json': 4, 'timestamp': 5, 'date': 6, 'boolean': 7,
             'character(10)': 8, 'character': 8}
    return _sort[val] if val in _sort else 0


def get_type(val):
    """
    Determines the PostgreSQL data type for a given Python value.

    Args:
        val: A Python value of any type.

    Returns:
        str: The corresponding PostgreSQL data type.
    """
    if val is None:
        return 'boolean'  # 'character(10)'
    if isinstance(val, bool):
        return 'boolean'
    if isinstance(val, str):
        return 'text'
    if isinstance(val, float):
        return 'real'
    if isinstance(val, int):
        if -9223372036854775805 > val or val > 9223372036854775805:
            return 'text'
        if -2147483645 > val or val > 2147483645:
            return 'bigint'
        return 'integer'
    if isinstance(val, (list, dict)):
        return 'json'
    if isinstance(val, datetime.datetime):
        return 'timestamp'
    if isinstance(val, datetime.date):
        return 'date'
    return 'text'


def guess_data_type(column: List):
    """
    Guesses the most appropriate data type for a column of values.

    Args:
        column (List): A list of values from a single column.

    Returns:
        str: The guessed PostgreSQL data type for the column.
    """
    v = {get_type(cell) for cell in column}
    v = list(sorted(v, key=lambda a: sort(a)))
    v = v[0]
    return v


def guess_table_schema(table: List[Dict]):
    """
    Guesses the schema for a table represented as a list of dictionaries.

    Args:
        table (List[Dict]): A list of dictionaries representing table rows.

    Returns:
        Dict[str, str]: A dictionary mapping column names to their guessed data types.
    """
    vv = {}
    for row in table:
        for k, v in row.items():
            if k not in vv:
                vv[k] = []
            vv[k].append(v)
    return {k: guess_data_type(v) for k, v in vv.items()}


class Postgres:
    """
    A class to handle PostgreSQL database operations.

    Attributes:
        host (str): The database host.
        port (str): The database port.
        user (str): The database user.
        password (str): The database password.
        database (str): The database name.
    """
    host: str = 'localhost'
    port: str = '5432'
    user: str = 'daniel'
    password: str = 'MyPassword123'
    database: str = 'my_db'

    def __init__(self, host=None, port=None, user=None, password=None, database=None):
        """
        Initializes the Postgres object with connection details.

        Args:
            host (str, optional): The database host.
            port (str, optional): The database port.
            user (str, optional): The database user.
            password (str, optional): The database password.
            database (str, optional): The database name.
        """
        self.host = host or self.host
        self.port = port or self.port
        self.user = user or self.user
        self.password = password or self.password
        self.database = database or self.database

    def query(self, query: str, *args):
        """
        Executes a SQL query.

        Args:
            query (str): The SQL query to execute.
            conn (connection, optional): An existing database connection to use.
            cur (cursor, optional): An existing database cursor to use.

        Returns:
            List[Tuple]: The result of the query.
        """
        with psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
        ) as conn:
            cur = conn.cursor()

            cur.execute(query, args)
            r = []
            try:
                r = cur.fetchall()
            except psycopg2.ProgrammingError: pass
            conn.commit()

            return r

    def download_table(self, table_name: str = None, columns='*', suffix='', sql=None):
        """
        Downloads data from a PostgreSQL table.

        Args:
            table_name (str, optional): The name of the table to download.
            columns (str, optional): The columns to select. Defaults to '*'.
            suffix (str, optional): Additional SQL to append to the query.
            sql (str, optional): A custom SQL query to execute instead of selecting from a table.

        Returns:
            List[Dict]: A list of dictionaries representing the table rows.
        """
        query = (sql or f'select {columns} from {table_name} {suffix};')

        with psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
        ) as conn:
            cur = conn.cursor()

            cur.execute(query)

            table = cur.fetchall()
            column_names = tuple(col[0] for col in cur.description)

            def check_values(values):
                def check_value(value):
                    if isinstance(value, str):
                        value = value.replace("''", "'")
                    return value
                return [check_value(value) for value in values]

            return [dict(zip(column_names, check_values(row))) for row in table]

    def upload_table(self, table_name: str, table: List[Dict], partition: str = None, partition_type: str = 'LIST', id_column=None):
        """
        Uploads data to a PostgreSQL table.

        Args:
            table_name (str): The name of the table to upload to.
            table (List[Dict]): The data to upload, as a list of dictionaries.
            partition (str, optional): The column to use for partitioning.
            partition_type (str, optional): The type of partitioning to use. Defaults to 'LIST'.
            conn (connection, optional): An existing database connection to use.
            cur (cursor, optional): An existing database cursor to use.
            id_column (str, optional): The name of the ID column.
        """
        def convert_value(value):
            if isinstance(value, str):
                return value.replace("'", "''")
            if isinstance(value, (int, float, bool, type(None), datetime.datetime)):
                return value
            elif isinstance(value, (tuple, list, dict)):
                return f"{json.dumps(value)}"
            raise ValueError(f'cannot place {value} in type "{type(value)}"')

        # get all headers
        keys = {}
        for row in table:
            for key in row:
                if key not in keys:
                    keys[key] = None

        # put headers in order, giving None values to missing headers
        for i, row in enumerate(table):
            table[i] = {k: convert_value(row.get(k, None)) for k in keys}

        with psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
        ) as conn:
            cur = conn.cursor()
            self.check_for_table(conn, cur, table_name, table, id_column, partition, partition_type)

            keys = ', '.join([f'"{k}"' for k in table[0].keys()])
            vals = ', '.join(['%s'] * len(table[0].keys()))
            q = f'INSERT INTO {table_name} ({keys}) VALUES ({vals})'
            table = (tuple(row.values()) for row in table)

            psycopg2.extras.execute_batch(cur, q, table)

            conn.commit()

    def table_schema(self, table_name: str, cur):
        """
        Retrieves the schema of a table.

        Args:
            table_name (str): The name of the table.
            cur: A database cursor.

        Returns:
            List[Tuple]: A list of tuples containing column names and data types.
        """
        query = f"SELECT column_name, data_type FROM information_schema.columns where table_name = '{table_name}' ORDER BY ordinal_position;"
        cur.execute(query)
        return cur.fetchall()  # tuple(map(lambda row: row[0], cur.fetchall()))

    def check_for_table(self, conn, cur, table_name: str, table: List[Dict], id_column=None, partition: str = None,
                        partition_type: str = 'LIST', skip_alterations=False):
        """
        Checks if a table exists and creates or alters it as necessary.

        Args:
            conn: A database connection.
            cur: A database cursor.
            table_name (str): The name of the table to check.
            table (List[Dict]): The data to be inserted into the table.
            id_column (str, optional): The name of the ID column.
            partition (str, optional): The column to use for partitioning.
            partition_type (str, optional): The type of partitioning to use. Defaults to 'LIST'.
            skip_alterations (bool, optional): If True, skips table alterations. Defaults to False.
        """
        schema = guess_table_schema(table)

        cur.execute(
            f"SELECT EXISTS (SELECT 1 FROM pg_tables  WHERE tablename = '{table_name}') AS table_existence;")
        table_exists = cur.fetchall()[0][0]

        if not table_exists:
            try:
                primary = ''
                unique = ''
                if partition:
                    if id_column:
                        if id_column == partition:
                            unique = f', PRIMARY KEY ("{id_column}")'
                        else:
                            unique = f', PRIMARY KEY ("{id_column}", "{partition}")'
                else:
                    primary = ' primary key UNIQUE'
                    if id_column:
                        unique = f', UNIQUE ("{id_column}")'

                end = ';' if not isinstance(partition, str) else f' PARTITION BY {partition_type} ("{partition}");'

                def column_and_data_type(k, v):
                    if k == id_column and not partition:
                        return f'"{k}" {v}{primary}'
                    return f'"{k}" {v}'

                sql = f'''create table if not exists {table_name}({", ".join([column_and_data_type(k, v) for k, v in schema.items()])}{unique}){end}'''

                cur.execute(sql)
                conn.commit()
            except psycopg2.errors.UniqueViolation as e:
                print(e)
        elif not skip_alterations:
            db_schema = dict(self.table_schema(table_name, cur))
            conn.commit()

            for col in set(schema.keys()) ^ set(db_schema.keys()):
                if col in schema:
                    try:
                        cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {schema[col]};')
                        conn.commit()
                    except psycopg2.errors.DuplicateColumn as e:
                        print(e)
                elif col in db_schema:
                    pass

            for col in set(schema.keys()) & set(db_schema.keys()):
                if schema[col] != db_schema[col]:
                    if sort(schema[col]) < sort(db_schema[col]):
                        # print(f'comparing {sort(schema[col])} < {sort(db_schema[col])} == sort({schema[col]}) < sort({db_schema[col]})')
                        self.convert_table_column_type(table_name, f'"{col}"', schema[col], conn, cur)
                    elif sort(schema[col]) > sort(db_schema[col]):
                        pass

    def convert_table_column_type(self, table_name: str, column_name: str, new_type: str, conn, cur):
        """
        Converts the data type of a table column.

        Args:
            table_name (str): The name of the table.
            column_name (str): The name of the column to convert.
            new_type (str): The new data type for the column.
            conn: A database connection.
            cur: A database cursor.
        """
        base_values = {'text': "''", 'real': '0', 'bigint': '0', 'integer': '0', 'json': "''",
                     'timestamp': datetime.datetime.utcnow(), 'date': "'2023-01-01'", 'boolean': 'false', 'character(10)': "''"}
        try:
            cur.execute(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_type};')
            conn.commit()
        except psycopg2.errors.DatatypeMismatch as e:
            base_value = base_values[new_type]
            try:
                cur.execute(f'ALTER TABLE {table_name} '
                            f'ALTER COLUMN {column_name} '
                            f'TYPE {new_type} '
                            f'USING (nullif({column_name}, {base_value}))::{new_type};')
                conn.commit()
            except:  # (psycopg2.errors.InFailedSqlTransaction, psycopg2.errors.CannotCoerce) as e:
                cur.execute(f'ALTER TABLE {table_name} '
                            f'ALTER COLUMN {column_name} '
                            f'TYPE {new_type} '
                            f'USING {column_name}::text::{new_type};')
                conn.commit()


# from __future__ import annotations
# import psycopg2
# import psycopg2.extensions
# import psycopg2.extras
# import psycopg2.errors
# # from psycopg2.extras import execute_batch
# import os
# import json
# import datetime
# from typing import Union, List, Dict, Any, Optional, Callable
#
# try:
#     import dotenv
#     dotenv.load_dotenv()
# except ModuleNotFoundError:
#     pass
#
# def get_postgres_from_env() -> Postgres:
#     """
#     This function returns a Postgres object with the credentials from the environment variables.
#     If the environment variables are not set, it returns a Postgres object with the default values.
#
#     Return:  Postgres
#     """
#     return Postgres(
#         host=os.environ.get('POSTGRES_HOST', 'localhost'),
#         port=os.environ.get('POSTGRES_PORT', '5432'),
#         user=os.environ.get('POSTGRES_USER', 'daniel'),
#         password=os.environ.get('POSTGRES_PASSWORD', 'MyPassword123'),
#         database=os.environ.get('POSTGRES_DATABASE', 'my_db')
#     )
#
#
# def sort(val):
#     _sort = {'text': 0, 'real': 1, 'bigint': 2, 'integer': 3, 'json': 4, 'timestamp': 5, 'date': 6, 'boolean': 7,
#              'character(10)': 8, 'character': 8}
#     return _sort[val] if val in _sort else 0
#
#
# def get_type(val):
#     if val is None:
#         return 'boolean'  # 'character(10)'
#     if isinstance(val, bool):
#         return 'boolean'
#     if isinstance(val, str):
#         return 'text'
#     if isinstance(val, float):
#         return 'real'
#     if isinstance(val, int):
#         if -9223372036854775805 > val or val > 9223372036854775805:
#             return 'text'
#         if -2147483645 > val or val > 2147483645:
#             return 'bigint'
#         return 'integer'
#     if isinstance(val, (list, dict)):
#         return 'json'
#     if isinstance(val, datetime.datetime):
#         return 'timestamp'
#     if isinstance(val, datetime.date):
#         return 'date'
#     return 'text'
#
#
# def guess_data_type(column: List):
#     v = {get_type(cell) for cell in column}
#     v = list(sorted(v, key=lambda a: sort(a)))
#     v = v[0]
#     return v
#
#
# def guess_table_schema(table: List[Dict]):
#     vv = {}
#     for row in table:
#         for k, v in row.items():
#             if k not in vv:
#                 vv[k] = []
#             vv[k].append(v)
#     return {k: guess_data_type(v) for k, v in vv.items()}
#
#
# class Postgres:
#     host: str = 'localhost'
#     port: str = '5432'
#     user: str = 'daniel'
#     password: str = 'MyPassword123'
#     database: str = 'my_db'
#
#     def __init__(self, host=None, port=None, user=None, password=None, database=None):
#         self.host = host or self.host
#         self.port = port or self.port
#         self.user = user or self.user
#         self.password = password or self.password
#         self.database = database or self.database
#
#     def download_table(self, table_name: str = None, columns='*', suffix='', sql=None):
#         query = (sql or f'select {columns} from {table_name} {suffix};')
#
#         with psycopg2.connect(
#                 host=self.host,
#                 port=self.port,
#                 user=self.user,
#                 password=self.password,
#                 database=self.database
#         ) as conn:
#             cur = conn.cursor()
#
#             cur.execute(query)
#
#             table = cur.fetchall()
#             column_names = tuple(col[0] for col in cur.description)
#
#             return [dict(zip(column_names, row)) for row in table]  # to_list_of_dict_table(column_names, table)
#
#     def upload_table(self, table_name: str, table: List[Dict], partition: str = None, partition_type: str = 'LIST',
#                      conn=None, cur=None,
#                      id_column=None):
#         def convert_value(value):
#             if isinstance(value, str):
#                 return value.replace("'", "''")
#             if isinstance(value, (int, float, bool, type(None), datetime.datetime)):
#                 return value
#             elif isinstance(value, (tuple, list, dict)):
#                 return f"{json.dumps(value)}"
#             raise ValueError(f'cannot place {value} in type "{type(value)}"')
#
#         # get all headers
#         keys = {}
#         for row in table:
#             for key in row:
#                 if key not in keys:
#                     keys[key] = None
#
#         # put headers in order, giving None values to missing headers
#         for i, row in enumerate(table):
#             table[i] = {k: convert_value(row.get(k, None)) for k in keys}
#
#         with psycopg2.connect(
#                 host=self.host,
#                 port=self.port,
#                 user=self.user,
#                 password=self.password,
#                 database=self.database
#         ) as conn:
#             cur = conn.cursor()
#             self.check_for_table(conn, cur, table_name, table, id_column, partition, partition_type)
#
#             keys = ', '.join([f'"{k}"' for k in table[0].keys()])
#             vals = ', '.join(['%s'] * len(table[0].keys()))
#             q = f'INSERT INTO {table_name} ({keys}) VALUES ({vals})'
#             table = (tuple(row.values()) for row in table)
#
#             psycopg2.extras.execute_batch(cur, q, table)
#
#             conn.commit()
#
#     def table_schema(self, table_name: str, cur):
#         query = f"SELECT column_name, data_type FROM information_schema.columns where table_name = '{table_name}' ORDER BY ordinal_position;"
#         cur.execute(query)
#         return cur.fetchall()  # tuple(map(lambda row: row[0], cur.fetchall()))
#
#     def check_for_table(self, conn, cur, table_name: str, table: List[Dict], id_column=None, partition: str = None,
#                         partition_type: str = 'LIST', skip_alterations=False):
#         schema = guess_table_schema(table)
#
#         cur.execute(
#             f"SELECT EXISTS (SELECT 1 FROM pg_tables  WHERE tablename = '{table_name}') AS table_existence;")
#         table_exists = cur.fetchall()[0][0]
#
#         if not table_exists:
#             try:
#                 e, primary = '', ' primary key UNIQUE' * (not partition)  # empty, extra
#                 unique = ''  # f', UNIQUE("{id_column}")' if id_column else ''
#                 if id_column:
#                     if partition:
#                         if id_column == partition:
#                             unique = f', PRIMARY KEY ("{id_column}")'
#                         else:
#                             unique = f', PRIMARY KEY ("{id_column}", "{partition}")'
#                     else:
#                         unique = f', UNIQUE("{id_column}")'
#                 end = ';' if not isinstance(partition, str) else f' PARTITION BY {partition_type} ("{partition}");'
#
#                 def ok(k, v):
#                     if partition:
#                         if k == id_column:
#                             return f'"{k}" {v}'
#                     if k == partition:
#                         return f'"{k}" {v}'
#                     return f'"{k}" {v}{primary if k == id_column else e}'
#
#                 sql = f'''create table if not exists {table_name}({", ".join([ok(k, v) for k, v in schema.items()])}{unique}){end}'''
#
#                 cur.execute(sql)
#                 conn.commit()
#             except psycopg2.errors.UniqueViolation as e:
#                 print(e)
#         elif not skip_alterations:
#             db_schema = dict(self.table_schema(table_name, cur))
#             conn.commit()
#
#             for col in set(schema.keys()) ^ set(db_schema.keys()):
#                 if col in schema:
#                     try:
#                         cur.execute(f'ALTER TABLE {table_name} ADD COLUMN "{col}" {schema[col]};')
#                         conn.commit()
#                     except psycopg2.errors.DuplicateColumn as e:
#                         print(e)
#                 elif col in db_schema:
#                     pass
#
#             for col in set(schema.keys()) & set(db_schema.keys()):
#                 if schema[col] != db_schema[col]:
#                     if sort(schema[col]) < sort(db_schema[col]):
#                         # print(f'comparing {sort(schema[col])} < {sort(db_schema[col])} == sort({schema[col]}) < sort({db_schema[col]})')
#                         self.convert_table_column_type(table_name, f'"{col}"', schema[col], conn, cur)
#                     elif sort(schema[col]) > sort(db_schema[col]):
#                         pass
#
#     def convert_table_column_type(self, table_name: str, column_name: str, new_type: str, conn, cur):
#         base_values = {'text': "''", 'real': '0', 'bigint': '0', 'integer': '0', 'json': "''",
#                      'timestamp': datetime.datetime.utcnow(), 'date': "'2023-01-01'", 'boolean': 'false', 'character(10)': "''"}
#         try:
#             cur.execute(f'ALTER TABLE {table_name} ALTER COLUMN {column_name} TYPE {new_type};')
#             conn.commit()
#         except psycopg2.errors.DatatypeMismatch as e:
#             base_value = base_values[new_type]
#             try:
#                 cur.execute(f'ALTER TABLE {table_name} '
#                             f'ALTER COLUMN {column_name} '
#                             f'TYPE {new_type} '
#                             f'USING (nullif({column_name}, {base_value}))::{new_type};')
#                 conn.commit()
#             except:  # (psycopg2.errors.InFailedSqlTransaction, psycopg2.errors.CannotCoerce) as e:
#                 cur.execute(f'ALTER TABLE {table_name} '
#                             f'ALTER COLUMN {column_name} '
#                             f'TYPE {new_type} '
#                             f'USING {column_name}::text::{new_type};')
#                 conn.commit()
