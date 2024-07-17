"""
This module provides utility functions for executing Python code and SQL queries dynamically.

It includes functions for running Python code from strings, executing PostgreSQL and SQLite queries,
and handling dynamic variable substitution in code strings.
"""
from __future__ import annotations
import os
import importlib
import contextlib
from typing import List, Dict, Any

import pipe_util
import postgres
import sqlite3_helper

import pipe_debug


class PipeLineException(Exception):
    """Custom exception for pipeline-related errors."""
    pass


@contextlib.contextmanager
def temp_mod(txt: str):
    """
    Create a temporary Python module from a string.

    Args:
        txt (str): The Python code to be written to the temporary module.

    Yields:
        str: The name of the temporary module.

    Raises:
        PipeLineException: If there's an error in creating or removing the temporary module.
    """
    mod = f'{pipe_util.get_id()}.py'
    try:
        with open(mod, 'w') as f:
            f.write(txt)
        yield mod.split('.')[0]
    finally:
        try:
            os.remove(mod)
        except:
            pass


def fix_data(data: List[Dict]) -> List[Dict]:
    """
    Ensure all dictionaries in a list have the same keys, filling missing keys with None.

    Args:
        data (List[Dict]): A list of dictionaries to fix.

    Returns:
        List[Dict]: The fixed list of dictionaries.
    """
    if isinstance(data, list) and len(data):
        keys = set()
        for row in data:
            keys |= set(row.keys())
        keys = list(data[0].keys()) + list(keys - set(data[0].keys()))
        for row in data:
            for k in keys:
                if k not in row:
                    row[k] = None
    return data


def get_index(k: str) -> tuple[int, str]:
    """
    Extract an index from a string key if present.

    Args:
        k (str): The key string to parse.

    Returns:
        tuple[int, str]: A tuple containing the extracted index (or 0 if not present) and the remaining key.
    """
    if not k.startswith('index['):
        return 0, k
    i, last = '', 0
    for index, c in enumerate(k[len('index['):]):
        last = index
        if c.isdigit():
            i += c
        elif c == ']':
            break
    return int(i), k[last+len('index[')+1:]


def apply_kwargs_to_txt(txt: str, kwargs: dict) -> str:
    """
    Apply keyword arguments to a text string, replacing placeholders.

    Args:
        txt (str): The text containing placeholders.
        kwargs (dict): The keyword arguments to apply.

    Returns:
        str: The text with placeholders replaced by their corresponding values.
    """
    order, skip = {}, {}
    for k, v in kwargs.items():
        i, k = get_index(k)
        if i not in order:
            order[i] = []
        order[i].append((k, v))
        skip[k] = max(skip.get(k, 0), i)
    for i in sorted(order.keys(), reverse=True):
        for k, v in order[i]:
            if skip[k] > i:
                continue
            txt = txt.replace('{{'+k+'}}', f'{v}')
    return txt


def place_null_values(txt: str) -> str:
    """
    Replace unfilled placeholders with a null value string.

    Args:
        txt (str): The text to process.

    Returns:
        str: The processed text with unfilled placeholders replaced.
    """
    return txt  # Currently a no-op, implement as needed


def get_end(txt: str, start: int, end_token: str) -> int:
    """
    Find the end index of a token in a string.

    Args:
        txt (str): The text to search.
        start (int): The starting index for the search.
        end_token (str): The token to search for.

    Returns:
        int: The index of the end token, or -1 if not found.
    """
    for i in range(start, len(txt)):
        if txt[i:i+len(end_token)] == end_token:
            return i
    return -1


def check_for_uuid_kwargs(txt: str, kwargs: dict) -> tuple[str, dict]:
    """
    Check for UUID placeholders in text and replace them with generated UUIDs.

    Args:
        txt (str): The text to process.
        kwargs (dict): Existing keyword arguments.

    Returns:
        tuple[str, dict]: The processed text and a dictionary of new UUID values.

    Raises:
        PipeLineException: If a UUID placeholder is not properly closed.
    """
    n = {}
    for start_token, end_token in [('{{uuid|', '}}'), ('{{uuid:', '}}')]:
        if start_token in txt:
            while start_token in txt:
                s = txt.index(start_token)
                e = get_end(txt, s, end_token)
                if e == -1:
                    raise PipeLineException('uuid not closed')
                _id = txt[s + len(start_token):e]
                key = f'uuid_{_id}'
                if key not in kwargs:
                    kwargs[key] = pipe_util.get_id()
                    n[key] = kwargs[key]
                v = kwargs[key]
                txt = txt[:s] + v + txt[e+2:]
    return txt, n


def check_py_output(data: Any) -> Any:
    """
    Check if the output of a Python execution is of a valid type.

    Args:
        data: The data to check.

    Returns:
        The input data if it's of a valid type.

    Raises:
        PipeLineException: If the data is of an invalid type.
    """
    if not isinstance(data, (list, dict, int, float, str, bool, type(None))):
        raise PipeLineException(f'invalid output type {type(data)} must be: (list, dict, int, float, str, bool, type(None))')
    return data


@pipe_debug.timeit
def run_py(txt: str, func: str, *args, __pure__=False, **kwargs) -> Any:
    """
    Run Python code from a string.

    Args:
        txt (str): The Python code to run.
        func (str): The name of the function to call in the code.
        *args: Positional arguments to pass to the function.
        __pure__ (bool): If True, return the raw output of the function.
        **kwargs: Keyword arguments to apply to the code.

    Returns:
        The result of the function execution, processed based on the __pure__ flag.

    Raises:
        PipeLineException: If the specified function is not found in the code.
    """
    txt = apply_kwargs_to_txt(txt, kwargs)
    txt, uuids = check_for_uuid_kwargs(txt, kwargs)
    txt = place_null_values(txt)

    with temp_mod(txt) as mod:
        module = importlib.import_module(mod)
        if hasattr(module, func):
            r = getattr(module, func)(*args)
            return r
        else:
            raise PipeLineException(f'function {func} not found.')
    return True, []


@pipe_debug.timeit
def run_postgres(txt: str, func: str, *args, **kwargs) -> Any:
    """
    Run a PostgreSQL query.

    Args:
        txt (str): The SQL query to run.
        func (str): The name of the function (used for table naming).
        *args: Data to be uploaded to temporary tables.
        **kwargs: Additional keyword arguments.

    Returns:
        The result of the SQL query execution.

    Raises:
        PipeLineException: If there's an error in query execution.
    """
    db: postgres.Postgres = postgres.get_postgres_from_env()
    tables = [pipe_util.get_id() for v in args]
    txt, uuids = check_for_uuid_kwargs(txt, kwargs)
    try:
        for i, data in enumerate(args):
            if data:
                n = func if i == 0 else f'pipe{i}'
                if n in txt:
                    try:
                        db.download_table(sql=f'DROP TABLE {tables[i]};')
                    except:
                        pass
                    db.upload_table(tables[i], data)
                    txt = txt.replace(n, tables[i])
        txt = apply_kwargs_to_txt(txt, {**kwargs})
        v = db.download_table(sql=txt)
        return v
    finally:
        for table in tables:
            try:
                db.download_table(f'DROP TABLE {table};')
            except: pass


@pipe_debug.timeit
def run_sqlite3(txt: str, func: str, *args, **kwargs) -> Any:
    """
    Run a SQLite query.

    Args:
        txt (str): The SQL query to run.
        func (str): The name of the function (used for table naming).
        *args: Data to be uploaded to temporary tables.
        **kwargs: Additional keyword arguments.

    Returns:
        The result of the SQL query execution.
    """
    db = sqlite3_helper.Sqlite3()
    tables = [pipe_util.get_id() for v in args]
    txt, uuids = check_for_uuid_kwargs(txt, kwargs)
    try:
        for i, data in enumerate(args):
            n = func if i == 0 else f'pipe{i}'
            if n in txt:
                try:
                    db.query(f'DROP TABLE {tables[i]};')
                except:
                    pass
                db.upload_table(tables[i], data)
                txt = txt.replace(n, tables[i])
        txt = apply_kwargs_to_txt(txt, {**kwargs})
        return db.download_table(sql=txt)
    finally:
        try:
            for table in tables:
                db.query(f'DROP TABLE {table};')
        except:
            pass


# from __future__ import annotations
# import asyncio, string
# import json
# import time
# import uuid, os, importlib, traceback, io
# from contextlib import contextmanager
# # from reindent import reindent
# # import helpful
# # from test4.my_json import JsonObject
# # from db import Postgres, s3_db
# import postgres
# import sqlite3_helper
# from typing import Union, List, Dict, Any, Optional, Callable
# import enum
#
# import pipe_util
#
#
# class PipeLineException(Exception):
#     """  """
#     # def __init__(self, msg: str):
#     #     self.msg = msg
#
#
# @contextmanager
# def temp_mod(txt: str):
#     mod = f'{pipe_util.get_id()}.py'
#     try:
#         with open(mod, 'w') as f:
#             f.write(txt)
#         yield mod.split('.')[0]
#     finally:
#         try:
#             os.remove(mod)
#         except:
#             pass
#
#
# def fix_data(data: List[Dict]):
#     if isinstance(data, list):
#         if len(data):
#             keys = set()
#             for row in data:
#                 keys |= set(row.keys())
#             keys = list(data[0].keys()) + list(keys - set(data[0].keys()))
#             for row in data:
#                 for k in keys:
#                     if k not in row:
#                         row[k] = None
#     return data
#
#
# def get_index(k: str):
#     if not k.startswith('index['):
#         return 0, k
#     i, last = '', 0
#     for index, c in enumerate(k[len('index['):]):
#         last = index
#         if c.isdigit():
#             i += c
#         elif c == ']':
#             break
#     return int(i), k[last+len('index[')+1:]
#
#
# def apply_kwargs_to_txt(txt: str, kwargs: dict):
#     order, skip = {}, {}
#     for k, v in kwargs.items():
#         i, k = get_index(k)
#         if i not in order:
#             order[i] = []
#         order[i].append((k, v))
#         skip[k] = max(skip.get(k, 0), i)
#     # print(sorted(order.keys(), reverse=True))
#     for i in sorted(order.keys(), reverse=True):
#         for k, v in order[i]:
#             if skip[k] > i:
#                 continue
#             # print('replacing', k, 'with', v)
#             txt = txt.replace('{{'+k+'}}', f'{v}')
#     # for k, v in kwargs.items():
#     #     txt = txt.replace('{{'+k+'}}', f'{v}')
#     return txt
#
#
# def place_null_values(txt: str):
#     return txt
#     none_value = 'Orphos_Pipeline_Null_Value / 0'
#     if '{{' in txt and '}}' in txt:
#         i = 0
#         chars = set(string.ascii_letters+string.digits+'_- ')
#         while txt.count('{{', i):
#             s = txt.index('{{', i)
#             e = txt.index('}}', i)
#             if not len(set(txt[s+2:e]) - chars):
#                 txt = txt[:s] + none_value + txt[e+2:]
#             i = e+2
#     return txt
#
#
# def get_end(txt: str, start: int, end_token: str):
#     for i in range(start, len(txt)):
#         if txt[i:i+len(end_token)] == end_token:
#             return i
#     return -1
#
#
# def check_for_uuid_kwargs(txt: str, kwargs: dict):
#     n = {}
#     for start_token, end_token in [('{{uuid|', '}}'), ('{{uuid:', '}}')]:
#         if start_token in txt:
#             while start_token in txt:
#                 s = txt.index(start_token)
#                 e = get_end(txt, s, end_token)
#                 if e == -1:
#                     raise PipeLineException('uuid not closed')
#                 _id = txt[s + len(start_token):e]  # DisplayPipeLines.trim(txt[s + len(start_token):e])
#                 key = f'uuid_{_id}'
#                 if key not in kwargs:
#                     kwargs[key] = pipe_util.get_id()
#                     n[key] = kwargs[key]
#                 v = kwargs[key]  # kwargs.get(f'uuid_{_id}', pipe_util.get_id())
#                 # print('replacing', key, 'with', v)
#                 txt = txt[:s] + v + txt[e+2:]
#     return txt, n
#
#
# def check_py_output(data):
#     if not isinstance(data, (list, dict, int, float, str, bool, type(None))):
#         raise PipeLineException(f'invalid output type {type(data)} must be: (list, dict, int, float, str, bool, type(None))')
#     return data
#     if not isinstance(data, list):
#         return []
#     if len(data):
#         if not isinstance(data[0], dict):
#             return []
#     return data
#
#
# def run_py(txt: str, func: str, *args, __pure__=False, **kwargs):
#     txt = apply_kwargs_to_txt(txt, kwargs)
#     # for k, v in kwargs.items():
#     #     txt = txt.replace('{{'+k+'}}', f'{v}')
#     txt, uuids = check_for_uuid_kwargs(txt, kwargs)
#     # print('setting null')
#     txt = place_null_values(txt)
#
#     with temp_mod(txt) as mod:
#         module = importlib.import_module(mod)
#         if hasattr(module, func):
#             r = getattr(module, func)(*args)
#             return r
#             if __pure__:
#                 return r
#             if isinstance(r, tuple):
#                 if len(r) == 2:
#                     return r[0], uuids, check_py_output(r[1])
#                 if len(r) == 4:
#                     return r[0], {**r[1], **uuids}, r[2] if isinstance(r[2], int) else 0, check_py_output(r[3])
#                 return r[0], {**r[1], **uuids}, check_py_output(r[2])
#             return True, uuids, check_py_output(r)
#             # return fix_data(getattr(module, func)(fix_data(data)))
#         else:
#             raise PipeLineException(f'function {func} not found.')
#     return True, []
#
#
# def run_postgres(txt: str, func: str, *args, **kwargs):
#     db: postgres.Postgres = postgres.get_postgres_from_env()
#
#     tables = [pipe_util.get_id() for v in args]
#     txt, uuids = check_for_uuid_kwargs(txt, kwargs)
#     # print('args', args)
#     try:
#         extras = {}
#         if len(args) > 0:
#             extras[func] = tables[0]
#         for i, data in enumerate(args):
#             if data:  # len(data) if isinstance(data, list) else 0:
#                 n = func if i == 0 else f'pipe{i}'
#                 if n in txt:
#                     db.upload_table(tables[i], data)
#                 # txt = txt.replace(n, tables[i])
#                 extras[n] = tables[i]
#
#         txt = apply_kwargs_to_txt(txt, {**kwargs, **extras})
#
#         v = db.download_table(sql=txt)
#
#         # print('v ->', v)
#         return v
#         return True, uuids, v
#     except:
#         raise #PipeLineException(traceback.format_exc())
#     finally:
#         try:
#             for table in tables:
#                 db.query(f'DROP TABLE {table};')
#         except:
#             pass
#
#
# def run_sqlite3(txt: str, func: str, *args, **kwargs):
#     db = sqlite3_helper.Sqlite3()
#     tables = [pipe_util.get_id() for v in args]
#     txt, uuids = check_for_uuid_kwargs(txt, kwargs)
#     try:
#         extras = {}
#         for i, data in enumerate(args):
#             n = func if i == 0 else f'pipe{i}'
#             if n in txt:
#                 db.upload_table(n, data)
#             extras[n] = tables[i]
#         txt = apply_kwargs_to_txt(txt, {**kwargs, **extras})
#         return db.download_table(sql=txt)
#         return True, uuids, db.download_table(sql=txt)
#     finally:
#         try:
#             for table in tables:
#                 db.query(f'DROP TABLE {table};')
#         except:
#             pass