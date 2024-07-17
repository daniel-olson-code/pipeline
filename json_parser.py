"""
This module provides JSON serialization and deserialization functions.

It attempts to use the high-performance 'orjson' library if available,
falling back to the standard 'json' library if 'orjson' is not installed.
"""

try:
    import orjson
except ModuleNotFoundError:
    print('please "pip install orjson" for performance.')
    import json

    class orjson:
        """
        A fallback class that mimics the orjson interface using the standard json library.
        This is used when orjson is not installed.
        """

        @staticmethod
        def dumps(*args, **kwargs):
            """
            Serialize object to JSON bytes.

            Args:
                *args: Positional arguments passed to json.dumps.
                **kwargs: Keyword arguments passed to json.dumps.

            Returns:
                bytes: JSON-encoded bytes.
            """
            return json.dumps(*args, **kwargs).encode()

        @staticmethod
        def loads(*args, **kwargs):
            """
            Deserialize JSON bytes or str to Python objects.

            Args:
                *args: Positional arguments. The first argument is expected to be JSON data.
                **kwargs: Keyword arguments passed to json.loads.

            Returns:
                The deserialized Python object.
            """
            if isinstance(args[0], bytes):
                args = (args[0].decode(), *args[1:])
            return json.loads(*args, **kwargs)


def dumps(*args, **kwargs):
    """
    Serialize object to JSON bytes.

    This function uses orjson.dumps if available, otherwise falls back to the
    custom orjson class which uses the standard json library.

    Args:
        *args: Positional arguments passed to orjson.dumps.
        **kwargs: Keyword arguments passed to orjson.dumps.

    Returns:
        bytes: JSON-encoded bytes.
    """
    return orjson.dumps(*args, **kwargs)


def loads(*args, **kwargs):
    """
    Deserialize JSON bytes or str to Python objects.

    This function uses orjson.loads if available, otherwise falls back to the
    custom orjson class which uses the standard json library.

    Args:
        *args: Positional arguments. The first argument is expected to be JSON data.
        **kwargs: Keyword arguments passed to orjson.loads.

    Returns:
        The deserialized Python object.
    """
    return orjson.loads(*args, **kwargs)

# try:
#     import orjson
# except ModuleNotFoundError:
#     print('please "pip install orjson" for performance.')
#     import json
#     class orjson:
#         @staticmethod
#         def dumps(*args, **kwargs):
#             return json.dumps(*args, **kwargs).encode()
#         @staticmethod
#         def loads(*args, **kwargs):
#             if isinstance(args[0], bytes):
#                 args = (args[0].decode(), *args[1:])
#             return json.loads(*args, **kwargs)
#
#
# def dumps(*args, **kwargs):
#     return orjson.dumps(*args, **kwargs)
#
#
# def loads(*args, **kwargs):
#     return orjson.loads(*args, **kwargs)