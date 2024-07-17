"""
This module implements a socket server for sending and receiving byte data using keys.

The server allows clients to set, get, and delete data associated with specific keys.
Data is stored in files within a '.bucket' directory.
"""

import socket
import threading
import os

try:
    import dotenv
    dotenv.load_dotenv()
except ModuleNotFoundError:
    pass

import pipe_debug

# Environment variables for client and server configuration
BUCKET_CLIENT_HOST: str = os.environ.get('BUCKET_CLIENT_HOST', 'localhost')
BUCKET_CLIENT_PORT: int = int(os.environ.get('BUCKET_CLIENT_PORT', 61535))

BUCKET_SERVER_HOST: str = os.environ.get('BUCKET_SERVER_HOST', '0.0.0.0')
BUCKET_SERVER_PORT: int = int(os.environ.get('BUCKET_SERVER_PORT', 61535))

BUCKET_END_TOKEN = b'[-_-]'
BUCKET_SPLIT_TOKEN = b'[*BUCKET_SPLIT_TOKEN*]'

save_path = '.bucket'

database: dict[str, bytes] = {}

if not os.path.exists(save_path):
    os.makedirs(save_path)


@pipe_debug.timeit
def receive(conn: socket.socket) -> bytes:
    """
    Receive data from a socket connection until the end marker is found.

    Args:
        conn (socket.socket): The socket connection to receive data from.

    Returns:
        bytes: The received data without the end marker.
    """
    data = b''
    while not data.endswith(BUCKET_END_TOKEN):
        v = conn.recv(1024)
        data += v
    token_len = len(BUCKET_END_TOKEN)
    return data[:-token_len]


@pipe_debug.timeit
def send(conn: socket.socket, data: bytes) -> None:
    """
    Send data through a socket connection with an end marker.

    Args:
        conn (socket.socket): The socket connection to send data through.
        data (bytes): The data to be sent.
    """
    conn.sendall(data + BUCKET_END_TOKEN)


class Client:
    """A client for interacting with the bucket server."""
    PORT: int
    HOST: str

    def __init__(self):
        """Initialize the client with host and port from environment variables."""
        self.PORT = BUCKET_CLIENT_PORT
        self.HOST = BUCKET_CLIENT_HOST

    @pipe_debug.timeit
    def set(self, key: str, data: bytes) -> None:
        """
        Set data for a given key on the server.

        Args:
            key (str): The key to associate with the data.
            data (bytes): The data to store.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(60. * 5.)
            s.connect((self.HOST, self.PORT))

            send(s, BUCKET_SPLIT_TOKEN.join([key.encode('utf-8'), b'set', data]))
            receive(s)

            # send(s, key.encode('utf-8'))
            # receive(s)
            #
            # send(s, b'set')
            # receive(s)
            #
            # send(s, data)
            # receive(s)

    @pipe_debug.timeit
    def get(self, key: str) -> bytes | None:
        """
        Retrieve data for a given key from the server.

        Args:
            key (str): The key to retrieve data for.

        Returns:
            bytes or None: The retrieved data, or None if the key doesn't exist.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(60. * 5.)
            s.connect((self.HOST, self.PORT))

            send(s, BUCKET_SPLIT_TOKEN.join([key.encode('utf-8'), b'get', b'__null__']))

            # send(s, key.encode('utf-8'))
            # receive(s)
            #
            # send(s, b'get')

            data: bytes = receive(s)
            if data == b'__null__':
                return None
            return data

    @pipe_debug.timeit
    def delete(self, key: str) -> None:
        """
        Delete data for a given key on the server.

        Args:
            key (str): The key to delete data for.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            s.settimeout(60. * 5.)
            s.connect((self.HOST, self.PORT))

            send(s, BUCKET_SPLIT_TOKEN.join([key.encode('utf-8'), b'delete', b'__null__']))
            receive(s)

            # send(s, key.encode('utf-8'))
            # receive(s)
            #
            # send(s, b'delete')
            # receive(s)


def check_file_directory(path: str) -> None:
    """
    Ensure the directory for a file path exists, creating it if necessary.

    Args:
        path (str): The file path to check.
    """
    if os.path.dirname(path) == '' or os.path.dirname(path) == '.' or os.path.dirname(path) == '.\\' or os.path.dirname(path) == '/' or os.path.dirname(path) == '\\' or os.path.dirname(path) == './':  # os.path.dirname(path) in {'', '.', '/', '\\', './', '.\\'}:
        return
    if not os.path.exists(os.path.dirname(path)):
        os.makedirs(os.path.dirname(path))


@pipe_debug.timeit
def handle_client(conn: socket.socket) -> None:
    """
    Handle a client connection, processing set, get, and delete requests.

    Args:
        conn (socket.socket): The client connection socket.
    """
    global database, save_path
    k: bytes
    m: bytes
    data: bytes
    key: str
    method: str

    def ok():
        send(conn, b'ok')

    with conn:

        k, m, data = receive(conn).split(BUCKET_SPLIT_TOKEN)
        key = k.decode('utf-8')
        method = m.decode('utf-8')

        if method == 'set':
            # ok()
            # database[key] = data
            check_file_directory(os.path.join(save_path, key))
            with open(os.path.join(save_path, key), 'wb') as f:
                f.write(data)  # receive(conn))
            ok()
        elif method == 'get':
            # if key not in database:
            #     send(conn, b'__null__')
            # else:
            #     send(conn, database[key])
            if not os.path.exists(os.path.join(save_path, key)):
                send(conn, b'__null__')
            else:
                with open(os.path.join(save_path, key), 'rb') as f:
                    send(conn, f.read())
        elif method == 'delete':
            # if key in database:
            #     del database[key]
            try:
                os.remove(os.path.join(save_path, key))
            except FileNotFoundError:
                pass
            ok()


class Server:
    """A server for handling bucket storage requests."""
    PORT: int
    HOST: str

    def __init__(self):
        """Initialize the server with host and port from environment variables."""
        self.PORT = BUCKET_SERVER_PORT
        self.HOST = BUCKET_SERVER_HOST

    def loop(self):
        """
        Start the server loop, listening for and handling client connections.
        """
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.HOST, self.PORT))
            s.listen(10)
            while True:
                try:
                    conn, addr = s.accept()
                    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    conn.settimeout(60. * 5.)

                    threading.Thread(target=handle_client, args=(conn, )).start()
                except TimeoutError as e:
                    pass


def main() -> None:
    """
    Run the bucket server.
    """
    server: Server = Server()
    print('running bucket server', server.HOST, '@', server.PORT)
    server.loop()


if __name__ == '__main__':
    main()

# """
# This socket server send byte data to the server using a key.
#
# """
# import socket
# import threading
# import os
#
# try:
#     import dotenv
#     dotenv.load_dotenv()
# except ModuleNotFoundError:
#     pass
#
#
# BUCKET_CLIENT_HOST = os.environ.get('BUCKET_CLIENT_HOST', 'localhost')
# BUCKET_CLIENT_PORT = int(os.environ.get('BUCKET_CLIENT_PORT', 61535))
#
# BUCKET_SERVER_HOST = os.environ.get('BUCKET_SERVER_HOST', '0.0.0.0')
# BUCKET_SERVER_PORT = int(os.environ.get('BUCKET_SERVER_PORT', 61535))
#
# save_path = '.bucket'
#
# if not os.path.exists(save_path):
#     os.makedirs(save_path)
#
#
# def receive(conn):
#     data = b''
#     while not data.endswith(b'[-_-]'):
#         v = conn.recv(1024)
#         data += v
#     return data[:-5]
#
#
# def send(conn, data):
#     conn.sendall(data+b'[-_-]')
#
#
# class Client(object):
#     HOST = "localhost"  # The server's hostname or IP address
#     PORT = 61535  # The port used by the server
#
#     def __init__(self):
#         self.PORT = BUCKET_CLIENT_PORT
#         self.HOST = BUCKET_CLIENT_HOST
#
#     def set(self, key: str, data: bytes):
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#             s.settimeout(60. * 5.)
#             s.connect((self.HOST, self.PORT))
#
#             # s.sendall(get_key().encode('utf-8')+b'[-_-]')
#             send(s, key.encode('utf-8'))
#             receive(s)
#
#             send(s, b'set')
#             receive(s)
#
#             send(s, data)
#             receive(s)
#
#     def get(self, key: str):
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#             s.settimeout(60. * 5.)
#             s.connect((self.HOST, self.PORT))
#
#             send(s, key.encode('utf-8'))
#             receive(s)
#
#             send(s, b'get')
#
#             data = receive(s)
#             if data == b'__null__':
#                 return None
#             return data
#
#     def delete(self, key: str):
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#             s.settimeout(60. * 5.)
#             s.connect((self.HOST, self.PORT))
#
#             send(s, key.encode('utf-8'))
#             receive(s)
#
#             send(s, b'delete')
#             receive(s)
#
#
# def check_file_directory(path: str):
#     if os.path.dirname(path) in {'', '.', '/', '\\', './', '.\\'}:
#         return
#     if not os.path.exists(os.path.dirname(path)):
#         os.makedirs(os.path.dirname(path))
#
# def handle_client(conn, addr):
#     def ok():
#         send(conn, b'ok')
#     with conn:
#         key = receive(conn).decode('utf-8')
#         ok()
#         method = receive(conn).decode('utf-8')
#
#         if method == 'set':
#             ok()
#             check_file_directory(os.path.join(save_path, key))
#             with open(os.path.join(save_path, key), 'wb') as f:
#                 f.write(receive(conn))
#             ok()
#         elif method == 'get':
#             if not os.path.exists(os.path.join(save_path, key)):
#                 send(conn, b'__null__')
#             else:
#                 with open(os.path.join(save_path, key), 'rb') as f:
#                     send(conn, f.read())
#         elif method == 'delete':
#             try:
#                 os.remove(os.path.join(save_path, key))
#             except FileNotFoundError:
#                 pass
#             ok()
#
#
# class Server(object):
#     HOST = "0.0.0.0"  # Standard loopback interface address (localhost)
#     PORT = 61535  # Port to listen on (non-privileged ports are > 1023)
#
#     def __init__(self):
#         self.PORT = BUCKET_SERVER_PORT
#         self.HOST = BUCKET_SERVER_HOST
#
#     def loop(self):
#         global handle_client
#         with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#             s.bind((self.HOST, self.PORT))
#             # s.settimeout(60.*5.)
#             # s.setblocking(False)
#             s.listen(10)
#             while True:
#                 try:
#                     conn, addr = s.accept()
#                     conn.settimeout(60. * 5.)
#
#                     threading.Thread(target=handle_client, args=(conn, addr)).start()
#                 except TimeoutError as e:
#                     pass
#
#
# if __name__ == '__main__':
#     server = Server()
#     print('running', server.HOST, '@', server.PORT)
#     server.loop()
#
#
