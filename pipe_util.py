from __future__ import annotations
import json
import math
import uuid


def json_copy(obj):
    """Creates a deep copy of a JSON-serializable object.

    Args:
        obj: Any JSON-serializable object.

    Returns:
        A deep copy of the input object.
    """
    return json.loads(json.dumps(obj))


def get_id():
    """Generates a unique identifier.

    Returns:
        str: A unique identifier string.
    """
    return f'a{uuid.uuid1()}'.replace('-', '')


def trim(s: str):
    """Removes leading/trailing whitespace and quotes from a string.

    Args:
        s: The input string to trim.

    Returns:
        str: The trimmed string.
    """
    s = s.strip()
    if s[:1] == '"' and s[-1:] == '"':
        return s[1:-1]
    return s


def in_quotes(index, string=None, last=None):
    """Determines if a character in a string is within quotes.

    This function can be used in two ways:
    1. As a generator that yields information about each character in a string.
    2. To check a specific index in a string.

    Args:
        index: Either an integer index or the string to analyze.
        string: The string to analyze (if index is an integer).
        last: The previous quote state (used for continuation).

    Returns:
        If used as a generator, yields tuples (index, char, is_in_quotes).
        If used for a specific index, returns the updated quote state.
    """
    if isinstance(index, str):
        def gen():
            in_string = ['"', False]
            for i in range(len(index)):
                in_string = in_quotes(i, index, in_string)
                yield i, index[i], in_string[1]
        return gen()
    in_string = last
    for quote in ['"""', "'''", '"', "'"]:
        if string[index].startswith(quote):
            if in_string[1] and in_string[0] == quote:
                in_string[1] = False
                break
            elif not in_string[1]:
                in_string = [quote, True]
                break
    return in_string


def extract_json(txt: str):
    """Extracts the first complete JSON object from a string.

    Args:
        txt: The input string containing JSON.

    Returns:
        tuple: A tuple containing:
            - The index after the extracted JSON object.
            - The extracted JSON string.
    """
    s = ''
    i = 0
    if txt.count('{') > 0:
        n, started = 0, False
        for index, c, is_in_quotes in in_quotes(txt):
            if not is_in_quotes:
                if c == '{':
                    started = True
                    n += 1
                if c == '}':
                    n -= 1
            s += c
            i = index + 1
            if n == 0 and started:
                break
    return i, s


def safe_is_nan(v):
    """Checks if a value is NaN (Not a Number).

    This function safely checks whether the input value is NaN,
    handling cases where the input might not support the isnan() function.

    Args:
        v: The value to check. Can be of any type.

    Returns:
        bool: True if the value is NaN, False otherwise or if the check fails.

    Example:
        >>> safe_is_nan(float('nan'))
        True
        >>> safe_is_nan(5)
        False
        >>> safe_is_nan("not a number")
        False
    """
    try:
        return math.isnan(v)
    except:
        return False


def try_number(value, _type=float, on_fail_return_value=None, asignment=None, nan_allowed=False):
    """Attempts to convert a value to a number.

    Args:
        value: The value to convert.
        _type: The desired number type (default is float).
        on_fail_return_value: The value to return if conversion fails.
        asignment: An optional list to store the converted value.
        nan_allowed: Whether NaN values are allowed.

    Returns:
        The converted number, or on_fail_return_value if conversion fails.
    """
    try:
        v = _type(value)
        a = isinstance(asignment, list)
        if a:
            if len(asignment) < 1:
                asignment.append(v)
            else:
                asignment[0] = v
        if not nan_allowed and safe_is_nan(v):
            return on_fail_return_value
        return v if not a else True
    except:
        return on_fail_return_value


def try_json_loads(data: str, func=lambda a: a):
    """Attempts to parse a JSON string.

    Args:
        data: The string to parse as JSON.
        func: A function to apply to the data if parsing fails.

    Returns:
        The parsed JSON object, or the result of func(data) if parsing fails.
    """
    try:
        return json.loads(data)
    except:
        return func(data)


class PipeObject(object):
    """A base class for pipeline objects with JSON serialization support.

    This class provides methods for string representation and JSON conversion.
    """

    def __str__(self):
        """Returns a string representation of the object."""
        def display(value):
            if isinstance(value, str):
                return f"'{value}'"
            return str(value)

        values = [f'{name}={display(value)}'
                  for name, value in self.__dict__.items()]

        return f'{type(self).__name__}({", ".join(values)})'

    def __repr__(self):
        """Returns a string representation of the object."""
        return str(self)

    def to_json(self) -> dict:
        """Converts the object to a JSON-serializable dictionary.

        Returns:
            dict: A dictionary representation of the object.
        """
        return self.__dict__

    def from_json(self, json: dict):
        """Populates the object's attributes from a JSON dictionary.

        Args:
            json: A dictionary containing attribute values.

        Returns:
            self: The updated object instance.
        """
        self.__dict__ = json
        return self


# from __future__ import annotations
# import json
# import math
# import uuid
#
#
# def json_copy(obj):
#     return json.loads(json.dumps(obj))
#
#
# def get_id():
#     return f'a{uuid.uuid1()}'.replace('-', '')
#
#
# def trim(s: str):
#     s = s.strip()
#     if s[:1] == '"' and s[-1:] == '"':
#         return s[1:-1]
#     # if s[:1] == "'" and s[-1:] == "'":
#     #     return s[1:-1]
#     return s
#
#
# def in_quotes(index, string=None, last=None):
#     if isinstance(index, str):
#         def gen():
#             in_string = ['"', False]
#             for i in range(len(index)):
#                 in_string = in_quotes(i, index, in_string)
#                 yield i, index[i], in_string[1]
#         return gen()
#     in_string = last
#     for quote in ['"""', "'''", '"', "'"]:
#         if string[index].startswith(quote):
#             if in_string[1] and in_string[0] == quote:
#                 in_string[1] = False
#                 break
#             elif not in_string[1]:
#                 in_string = [quote, True]
#                 break
#     return in_string
#
#
# def extract_json(txt: str):
#     s = ''
#     i = 0
#     if txt.count('{') > 0:
#         n, started = 0, False
#         for index, c, is_in_quotes in in_quotes(txt):
#             if not is_in_quotes:
#                 if c == '{':
#                     started = True
#                     n += 1
#                 if c == '}':
#                     n -= 1
#             s += c
#             i = index + 1
#             if n == 0 and started:
#                 break
#     return i, s
#
#
# def try_isnan(v):
#     try:
#         return math.isnan(v)
#     except:
#         return False
#
#
# def try_number(value, _type=float, on_fail_return_value=None, asignment=None, nan_allowed=False):
#     try:
#         v = _type(value)
#         a = isinstance(asignment, list)
#         if a:
#             if len(asignment) < 1:
#                 asignment.append(v)
#             else:
#                 asignment[0] = v
#         if not nan_allowed and try_isnan(v):
#             return on_fail_return_value
#         return v if not a else True
#     except:
#         return on_fail_return_value
#
#
# def try_json_loads(data: str, func=lambda a: a):
#     """
#     data: str
#     on fail => return func(data)
#     on success => return json.loads(data)
#     func: function(data) => Any
#     returns: dict or data
#     """
#     try:
#         return json.loads(data)
#     except:
#         return func(data)
#
#
# class PipeObject(object):
#     def __str__(self):
#         def display(value):
#             if isinstance(value, str):
#                 return f"'{value}'"
#             return str(value)
#         values = [f'{name}={display(value)}'
#                   for name, value in self.__dict__.items()]
#         return (f'step.Step('
#                 + ', '.join(values)
#                 + f')')
#
#     def __repr__(self):
#         return str(self)
#
#     def to_json(self) -> dict:
#         return self.__dict__
#
#     def from_json(self, json: dict):
#         self.__dict__ = json
#         return self
#
#
# # class PipeLineException(Exception):
# #     """  """
# #     # def __init__(self, msg: str):
# #     #     self.msg = msg
# #
# #
# # @contextmanager
# # def temp_mod(txt: str):
# #     mod = f'{name()}.py'
# #     try:
# #         with open(mod, 'w') as f:
# #             f.write(txt)
# #         yield mod.split('.')[0]
# #     finally:
# #         try:
# #             os.remove(mod)
# #         except:
# #             pass
# #
# #
# # def fix_data(data: List[Dict]):
# #     if isinstance(data, list):
# #         if len(data):
# #             keys = set()
# #             for row in data:
# #                 keys |= set(row.keys())
# #             keys = list(data[0].keys()) + list(keys - set(data[0].keys()))
# #             for row in data:
# #                 for k in keys:
# #                     if k not in row:
# #                         row[k] = None
# #     return data
# #
# #
# # def get_index(k: str):
# #     if not k.startswith('index['):
# #         return 0, k
# #     i, last = '', 0
# #     for index, c in enumerate(k[len('index['):]):
# #         last = index
# #         if c.isdigit():
# #             i += c
# #         elif c == ']':
# #             break
# #     return int(i), k[last+len('index[')+1:]
# #
# #
# # def apply_kwargs_to_txt(txt: str, kwargs: dict):
# #     order, skip = {}, {}
# #     for k, v in kwargs.items():
# #         i, k = get_index(k)
# #         if i not in order:
# #             order[i] = []
# #         order[i].append((k, v))
# #         skip[k] = max(skip.get(k, 0), i)
# #     # print(sorted(order.keys(), reverse=True))
# #     for i in sorted(order.keys(), reverse=True):
# #         for k, v in order[i]:
# #             if skip[k] > i:
# #                 continue
# #             # print('replacing', k, 'with', v)
# #             txt = txt.replace('{{'+k+'}}', f'{v}')
# #     # for k, v in kwargs.items():
# #     #     txt = txt.replace('{{'+k+'}}', f'{v}')
# #     return txt
# #
# #
# # def place_null_values(txt: str):
# #     return txt
# #     none_value = 'Orphos_Pipeline_Null_Value / 0'
# #     if '{{' in txt and '}}' in txt:
# #         i = 0
# #         chars = set(string.ascii_letters+string.digits+'_- ')
# #         while txt.count('{{', i):
# #             s = txt.index('{{', i)
# #             e = txt.index('}}', i)
# #             if not len(set(txt[s+2:e]) - chars):
# #                 txt = txt[:s] + none_value + txt[e+2:]
# #             i = e+2
# #     return txt
# #
# #
# # def get_end(txt: str, start: int, end_token: str):
# #     for i in range(start, len(txt)):
# #         if txt[i:i+len(end_token)] == end_token:
# #             return i
# #     return -1
# #
# #
# # def check_for_uuid_kwargs(txt: str, kwargs: dict):
# #     n = {}
# #     for start_token, end_token in [('{{uuid|', '}}'), ('{{uuid:', '}}')]:
# #         if start_token in txt:
# #             while start_token in txt:
# #                 s = txt.index(start_token)
# #                 e = get_end(txt, s, end_token)
# #                 if e == -1:
# #                     raise PipeLineException('uuid not closed')
# #                 _id = txt[s + len(start_token):e]  # DisplayPipeLines.trim(txt[s + len(start_token):e])
# #                 key = f'uuid_{_id}'
# #                 if key not in kwargs:
# #                     kwargs[key] = name()
# #                     n[key] = kwargs[key]
# #                 v = kwargs[key]  # kwargs.get(f'uuid_{_id}', name())
# #                 # print('replacing', key, 'with', v)
# #                 txt = txt[:s] + v + txt[e+2:]
# #     return txt, n
# #
# #
# # def check_py_output(data):
# #     if not isinstance(data, (list, dict, int, float, str, bool, type(None))):
# #         raise PipeLineException(f'invalid output type {type(data)} must be: (list, dict, int, float, str, bool, type(None))')
# #     return data
# #     if not isinstance(data, list):
# #         return []
# #     if len(data):
# #         if not isinstance(data[0], dict):
# #             return []
# #     return data
# #
# #
# # def run_py(txt: str, func: str, *args, __pure__=False, **kwargs):
# #     txt = apply_kwargs_to_txt(txt, kwargs)
# #     # for k, v in kwargs.items():
# #     #     txt = txt.replace('{{'+k+'}}', f'{v}')
# #     txt, uuids = check_for_uuid_kwargs(txt, kwargs)
# #     # print('setting null')
# #     txt = place_null_values(txt)
# #     with temp_mod(txt) as mod:
# #         module = importlib.import_module(mod)
# #         if hasattr(module, func):
# #             r = getattr(module, func)(*args)
# #             if __pure__:
# #                 return r
# #             if isinstance(r, tuple):
# #                 if len(r) == 2:
# #                     return r[0], uuids, check_py_output(r[1])
# #                 if len(r) == 4:
# #                     return r[0], {**r[1], **uuids}, r[2] if isinstance(r[2], int) else 0, check_py_output(r[3])
# #                 return r[0], {**r[1], **uuids}, check_py_output(r[2])
# #             return True, uuids, check_py_output(r)
# #             # return fix_data(getattr(module, func)(fix_data(data)))
# #         else:
# #             raise PipeLineException(f'function {func} not found.')
# #     return True, []
# #
# #
# # def run_postgres(txt: str, func: str, *args, **kwargs):
# #     db: postgres.Postgres = postgres.get_postgres_from_env()
# #
# #     tables = [name() for v in args]
# #     txt, uuids = check_for_uuid_kwargs(txt, kwargs)
# #     # print('args', args)
# #     try:
# #         extras = {}
# #         if len(args) > 0:
# #             extras['table'] = tables[0]
# #         for i, data in enumerate(args):
# #             if len(data) if isinstance(data, list) else 0:
# #                 n = 'table' if i == 0 else 'table'+str(i)  # '{{table}}' if i == 0 else '{{table'+str(i)+'}}'
# #                 # print('n', n)
# #                 if n in txt:
# #                     db.upload_table(tables[i], data)
# #                 # txt = txt.replace(n, tables[i])
# #                 extras[n] = tables[i]
# #         # for k, v in kwargs.items():
# #         #     txt = txt.replace('{{'+k+'}}', f'{v}')
# #         # print('kwargs', {**kwargs, **extras})
# #         # print('txt1 ->', txt)
# #         txt = apply_kwargs_to_txt(txt, {**kwargs, **extras})
# #         # txt = txt.replace('{{table}}', table)
# #         # print('txt2 ->', txt)
# #         v = []
# #         try:
# #             v = db.download_table(sql=txt)
# #         except:
# #             if kwargs.get('__try__', None) != 'true':
# #                 raise
# #         if '__try__' in kwargs:
# #             uuids = {**uuids, '__try__': 'false'}
# #         # print('v ->', v)
# #         return True, uuids, v
# #     except:
# #         raise #PipeLineException(traceback.format_exc())
# #     finally:
# #         try:
# #             for table in tables:
# #                 db.query(f'DROP TABLE {table};')
# #         except:
# #             pass
# #
# #
# # def run_sqlite3(txt: str, func: str, *args, **kwargs):
# #     # class SQL(): pass
# #     db = SQL()
# #     # table = name()
# #     tables = [name() for v in args]
# #     txt, uuids = check_for_uuid_kwargs(txt, kwargs)
# #     try:
# #         extras = {}
# #         # print('args', args)
# #         for i, data in enumerate(args):
# #             n = 'table' if i == 0 else 'table'+str(i)  # '{{table}}' if i == 0 else '{{table' + str(i) + '}}'
# #             # print('n', n)
# #             if n in txt:
# #                 # print('data', data)
# #                 db.upload_table(tables[i], data)
# #             # txt = txt.replace(n, tables[i])
# #             extras[n] = tables[i]
# #         # for i, data in enumerate(args):
# #         #     n = '{{table}}' if i == 0 else '{{table' + str(i) + '}}'
# #         #     if n in txt:
# #         #         db.upload_table(table, data)
# #         # for k, v in kwargs.items():
# #         #     txt = txt.replace('{{'+k+'}}', f'{v}')
# #         # txt = txt.replace('{{table}}', table)
# #         txt = apply_kwargs_to_txt(txt, {**kwargs, **extras})
# #         # print('txt ->', txt)
# #         return True, uuids, db.download_table(sql=txt)
# #     finally:
# #         try:
# #             for table in tables:
# #                 db.query(f'DROP TABLE {table};')
# #         except:
# #             pass
#
#
# # class Result(object):
# #     status: StepStatus = True
# #     env: dict = None
# #     priority: int = None
# #     scope: str = None
# #     velocity: float = None
# #     data: Any = None
# #
# #     def __init__(self, status = None, env = None, priority = None, scope = None, velocity = None, data = None,):
# #         self.status = status
# #         self.env = env or {}
# #         self.priority = priority
# #         self.scope = scope
# #         self.velocity = velocity
# #         self.data = data
# #
# #     @classmethod
# #     def from_result(cls, result):
# #         self = cls()
# #         status, kw, p, s, v, data = result
# #         self.status = status
# #         self.kwargs = kw
# #         self.priority = p
# #         self.scope = s
# #         self.velocity = v
# #         self.data = data
# #         return self
# #
# #
# # class StepStatus(enum.Enum):
# #     success = 1
# #     failed = 2
# #     pending = 3
# #     reset = 4
# #
# #
# # class Step(class_json_util.JsonObject):
# #     id: str = None
# #     name: str = 'empty'
# #     type: str = None
# #     code: str = None
# #     func: str = None
# #     local: str = False  # code found locally
# #     kwargs: dict = None
# #     caching = False
# #     cache = None
# #
# #     parents: list[str] = None
# #
# #     # parents: List[str] = None
# #     children: List[str] = None
# #
# #     types = ['python', 'postgres', 'pg', 'go', 'sqlite3', 'py']
# #     type_conversion = {
# #         'pg':  'postgres',
# #         'postgres': 'postgres',
# #         'postgresql': 'postgres',
# #         'sqlite3': 'sqlite3',
# #         'sql3': 'sqlite3',
# #         'py': 'python',  # 'python'
# #         'python': 'python',  # 'python
# #         'go': 'go',  # 'go'
# #     }
# #
# #     # def __init__(self, code: str, func: str, kwargs: dict):
# #     #     self.code = code
# #     #     self.func = func
# #     #     self.kwargs = kwargs
# #
# #     @classmethod
# #     def create_return_value(cls, value) -> Result:
# #         result = None
# #         if isinstance(value, Result):
# #             return value
# #         if isinstance(value, list):
# #             result = StepStatus.success, None, None, None, None, value
# #         if isinstance(value, tuple):
# #             if len(value) == 2:
# #                 result = value[0], None, None, None, None, value[1]
# #             if len(value) == 3:
# #                 result = value[0], value[1], None, None, None, value[2]
# #                 # result = value[0], value[1], None, None, None, Data({'value': value[2]})
# #             if len(value) == 4:
# #                 result = value[0], value[1], value[2], None, None, value[3]  # Data({'value': value[2]})
# #                 # result = value[0], value[1], value[2], None, None, Data({'value': value[2]})
# #             if len(value) == 5:
# #                 result = value[0], value[1], value[2], value[3], None, value[4]  # Data({'value': value[2]})
# #                 #  result = value[0], value[1], value[2], value[3], None, Data({'value': value[2]})
# #             if len(value) == 6:
# #                 result = value[0], value[1], value[2], value[3], value[4], value[5]  # Data({'value': value[2]})
# #                 #  result = value[0], value[1], value[2], value[3], value[4], Data({'value': value[2]})
# #         # if self.caching:
# #         #     self.cache = result
# #         if result is None:
# #             raise ValueError('value is not a list or tuple.')
# #         return Result.from_result(result)
# #
# #     def get_code(self):
# #         code = self.code
# #         if self.local:
# #             with open(self.code, 'r') as f:
# #                 code = f.read()
# #         # code = pipelines.apply_properties(code, skip=skip)
# #         return code
# #
# #     def run(self, data: Any) -> Result:  # tuple[bool, dict, Data]:
# #         # if self.caching:
# #         #     if isinstance(self.cache, tuple):
# #         #         if self.cache[0]:
# #         #             return self.cache
# #         print('stepping', self.name)
# #         # add_dicts(pipelines.kwargs, self.kwargs)
# #         code = self.get_code()
# #         # self.code
# #         # if self.local:
# #         #     with open(self.code, 'r') as f:
# #         #         code = f.read()
# #         # if isinstance(data, Data):
# #         #     args = [data.value]
# #         # elif isinstance(data, list):
# #         #     args = [d.value for d in data]
# #         # else:
# #         #     raise TypeError('data is not a Data or list.')
# #         # print('type', self.type)
# #         args = [data]
# #         if self.type == 'POSTGRESQL':
# #             return self.create_return_value(run_postgres(code, self.func, *args, **self.kwargs))  # True, Data({'value': run_postgres(code, self.func, data.value, **self.kwargs)})
# #         if self.type == 'PYTHON':
# #             # done, value = run_py(code, self.func, data.value, **self.kwargs)
# #             return self.create_return_value(run_py(code, self.func, *args, **self.kwargs))  # done, Data({'value': value})
# #         if self.type == 'SQLITE3':
# #             return self.create_return_value(run_sqlite3(code, self.func, *args, **self.kwargs))  # True, Data({'value': run_sqlite3(code, self.func, data.value, **self.kwargs)})
# #
