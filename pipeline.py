import sqlite3
import os
import json
import socket
import threading
import queue
import time
from typing import Any

import step
import bucket
import json_parser
import pipe_interpreter

try:
    import dotenv
    dotenv.load_dotenv()
except ModuleNotFoundError:
    pass

import pipe_debug


PIPELINE_HOST = os.environ.get('PIPELINE_HOST', '0.0.0.0')
PIPELINE_PORT = int(os.environ.get('PIPELINE_PORT', 65432))

db_path = os.path.join('database.db')

bucket_client = bucket.Client()

# Initialize a global tag_usage dictionary
tag_usage = {}
tag_lock = threading.Lock()

# Create a queue to handle incoming connections
connection_queue = queue.Queue()

PIPELINE_SPLIT_TOKEN = b'|-**-|'


def load_db():
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute('PRAGMA journal_mode=WAL')
        cur.execute('CREATE TABLE IF NOT EXISTS steps ('
                    'id, priority, scope, velocity, tag, status, epoch, msg, trace);')
        cur.execute('CREATE TABLE IF NOT EXISTS tags ('
                    'tag, velocity);')
        conn.commit()


@pipe_debug.timeit
def upload_step(_step: step.Step, status: step.StepStatus) -> None:
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        # upsert step into sqlite3 db. example:
        # INSERT INTO t1(id, c)
        # VALUES (1, 'c')
        # ON CONFLICT(id) DO UPDATE SET c = excluded.c;
        # cur.execute('INSERT INTO steps (id, priority, scope, velocity, tag, status, epoch, msg, trace) '
        #             'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) '
        #             'ON CONFLICT(id) DO UPDATE SET priority = excluded.priority, '
        #             'scope = excluded.scope, velocity = excluded.velocity, '
        #             'tag = excluded.tag, status = excluded.status, epoch = excluded.epoch, '
        #             'msg = excluded.msg, trace = excluded.trace;', (_step.id, _step.priority, _step.scope,
        #                                                             _step.velocity, _step.tag, status.value,
        #                                                             time.time(), '', ''))

        sql = ('INSERT INTO steps (id, priority, scope, velocity, tag, status, epoch, msg, trace) '
               'VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);')
        cur.execute(sql, (_step.id, _step.priority, _step.scope, _step.velocity, _step.tag, f'{status.value}',
                          time.time(), '', ''))
        conn.commit()


@pipe_debug.timeit
def upload_pipe_code(code: str):
    variables = pipe_interpreter.get_steps_from_code(code)
    steps = variables['steps']
    starters = variables['starters']
    print('uploading', len(steps), 'steps')
    for _step in steps.values():
        set_step(_step)
        status = step.StepStatus.pending if _step.id in starters else step.StepStatus.queued
        upload_step(_step, status)


@pipe_debug.timeit
def upload_pipe_code_from_file(file_path: str):
    with open(file_path, 'r') as f:
        code = f.read()
        upload_pipe_code(code)


@pipe_debug.timeit
def get_step(step_id: str) -> step.Step:
    s = step.Step()
    b = bucket_client.get(f'step/{step_id}')
    data = json_parser.loads(b)
    return s.from_json(data)


@pipe_debug.timeit
def set_step(_step: step.Step) -> None:
    b = json_parser.dumps(_step.to_json())
    bucket_client.set(f'step/{_step.id}', b)


@pipe_debug.timeit
def get_data(step_id: str) -> Any:
    key = f'step-data/{step_id}'
    v = bucket_client.get(key)
    if v is None:
        raise ValueError(f'No data found for step {step_id}')
    return json_parser.loads(v)


@pipe_debug.timeit
def set_data(step_id: str, data: Any) -> None:
    key = f'step-data/{step_id}'
    b = json_parser.dumps(data)
    bucket_client.set(key, b)


@pipe_debug.timeit
def remove_data(step_id: str) -> None:
    key = f'step-data/{step_id}'
    bucket_client.delete(key)


# def get_steps(scopes: list[str], limit=50):
#     """
#     Get the steps in the scope, ordered by priority then scope position.
#
#     Args:
#         scopes (list[str]): A list of scopes to filter the steps.
#         limit (int): The maximum number of steps to retrieve. Defaults to 50.
#
#     Returns:
#         list: A list of steps ordered by priority and scope position.
#     """
#     case_statement = ' '.join([f"WHEN ? THEN {i}" for i in range(len(scopes))])
#     sql = (f'SELECT id, priority, scope, velocity, data '
#            f'FROM steps '
#            f'WHERE scope IN ({",".join("?" * len(scopes))}) '
#            f'AND status = \'{step.StepStatus.pending.value}\' '
#            f'ORDER BY priority, CASE scope {case_statement} END LIMIT ?')
#     cur.execute(sql, (*scopes, *scopes, limit))
#     rows = cur.fetchall()
#     return [get_step(row[0]) for row in rows]


def check_to_delete_bucket_files(
        step_id: str,
        already: set | None = None,
        steps: list | None = None
) -> None:
    _check_to_delete_bucket_files(step_id, already, steps)
    # t = threading.Thread(target=_check_to_delete_bucket_files, args=(step_id, already, steps))
    # t.start()


def _check_to_delete_bucket_files(
        step_id: str,
        already: set | None = None,
        steps: list | None = None
) -> None:
    first_iteration = already is None and steps is None
    _step = get_step(step_id)
    already = set() if first_iteration else already
    steps = [] if first_iteration else steps
    already.add(_step.id)
    steps.append(_step)

    if _step.parents:
        for parent in _step.parents:
            if parent not in already:
                already.add(parent)
                check_to_delete_bucket_files(parent, already, steps)

    if _step.children:
        for child in _step.children:
            if child not in already:
                already.add(child)
                check_to_delete_bucket_files(child, already, steps)

    if first_iteration:
        ids = [s.id for s in steps]
        finished_statuses = {f'{v}' for v in [step.StepStatus.cancel.value, step.StepStatus.success.value]}
        with sqlite3.connect(db_path) as conn:
            cur = conn.cursor()
            sql = f'SELECT status FROM steps WHERE id IN ({", ".join("?" * len(ids))})'
            cur.execute(sql, ids)
            rows = cur.fetchall()

            if all([f'{row[0]}' in finished_statuses for row in rows]):
                for s in steps:
                    remove_data(s.id)


@pipe_debug.timeit
def _done(step_id: str):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        _step = get_step(step_id)
        # set_data(_step.id, result.data)
        sql_update_step = (f'UPDATE steps SET status = \'{step.StepStatus.success.value}\', epoch = ? WHERE id = ?')
        cur.execute(sql_update_step, (time.time(), _step.id))

        # cur.execute(sql_set_status, (_step.id, ))
        conn.commit()

        if _step.children:
            sql_update_children = (f'UPDATE steps SET status = \'{step.StepStatus.pending.value}\', epoch = ? WHERE id IN ({", ".join("?" * len(_step.children))})')
            cur.execute(sql_update_children, (time.time(), *_step.children))
            conn.commit()

        check_to_delete_bucket_files(step_id)


@pipe_debug.timeit
def _pending(step_id: str):
    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        sql_update_step = (f'UPDATE steps SET status = \'{step.StepStatus.pending.value}\', epoch = ? WHERE id = ?')
        cur.execute(sql_update_step, (time.time(), step_id))


@pipe_debug.timeit
def _cancel(
        step_id: str,
        already: set | None = None
) -> None:
    first_iteration = already is None
    _step = get_step(step_id)
    already = set() if first_iteration else already

    sql_update_step = f'UPDATE steps SET status = \'{step.StepStatus.cancel.value}\', epoch = ? WHERE id = ?'

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql_update_step, (time.time(), _step.id))
        conn.commit()

    if _step.parents:
        for parent in _step.parents:
            if parent not in already:
                already.add(parent)
                _cancel(parent, already)

    if _step.children:
        for child in _step.children:
            if child not in already:
                already.add(child)
                _cancel(child, already)

    if first_iteration:
        check_to_delete_bucket_files(step_id)


@pipe_debug.timeit
def _reset(step_id: str, already=None):
    _step = get_step(step_id)
    already = set() if not already else already
    status = step.StepStatus.pending.value if _step.parents else step.StepStatus.queued.value
    sql_update_step = f'UPDATE steps SET status = \'{status}\', epoch = ? WHERE id = ?'

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql_update_step, (time.time(), _step.id))
        conn.commit()

    if _step.children:
        for child in _step.children:
            if child not in already:
                already.add(child)
                _reset(child, already)

    if _step.parents:
        for parent in _step.parents:
            if parent not in already:
                already.add(parent)
                _reset(parent, already)


@pipe_debug.timeit
def _error(step_id: str, msg: str, trace: str):
    _step = get_step(step_id)
    sql_update_step = (f'UPDATE steps SET status = \'{step.StepStatus.error.value}\', epoch = ?, msg = ?, trace = ? WHERE id = ?')

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        cur.execute(sql_update_step, (time.time(), msg, trace, _step.id))
        conn.commit()


def get_steps(scopes: list, limit=50, chunk_size=100):
    """
    Get the steps in the scope, ordered by priority, velocity, then scope position.

    Args:
        scopes (list): A list of scopes to filter the steps.
        limit (int): The maximum number of steps to retrieve. Defaults to 50.
        chunk_size (int): The number of steps to fetch in each chunk. Defaults to 100.

    Returns:
        list: A list of steps ordered by priority, velocity, and scope position.
    """
    global tag_usage

    with sqlite3.connect(db_path) as conn:
        cur = conn.cursor()
        # Fetch velocities for each tag
        velocity_sql = 'SELECT tag, velocity FROM tags'  # f'SELECT tag, velocity FROM tags WHERE scope IN ({",".join("?" * len(scopes))})'
        cur.execute(velocity_sql)  # (velocity_sql, (*scopes,))
        tag_velocities = dict(cur.fetchall())

        # Initialize tag_usage for new tags
        for tag in tag_velocities:
            if tag not in tag_usage:
                tag_usage[tag] = 0

        case_statement = ' '.join([f"WHEN ? THEN {i}" for i in range(len(scopes))])
        offset = 0
        steps = []

        while len(steps) < limit:
            sql = (f'SELECT id, priority, scope, velocity, tag '
                   f'FROM steps '
                   f'WHERE scope IN ({",".join("?" * len(scopes))}) '
                   f'AND ('
                   f'   status = \'{step.StepStatus.pending.value}\' '
                   f'   or (epoch < {time.time() - (60 * 60 * 2)} and status = \'{step.StepStatus.working.value}\')'
                   f')'
                   f'ORDER BY CASE scope {case_statement} END, priority desc, epoch '  # , COALESCE(velocity, 1.0/0.0)
                   f'LIMIT ? OFFSET ?')
            cur.execute(sql, (*scopes, *scopes, chunk_size, offset))
            rows = cur.fetchall()
            if not rows:
                break  # Exit loop if no more rows are fetched

            for row in rows:
                step_id, priority, scope, velocity, tag = row
                if tag not in tag_velocities:
                    tag_velocities[tag] = None
                    tag_usage[tag] = 0
                if tag_velocities[tag] is None or tag_usage[tag] < tag_velocities[tag]:
                    steps.append(step_id)  # (get_step(step_id))
                    tag_usage[tag] += 1
                    if len(steps) >= limit:
                        break

            offset += chunk_size

        sql_set_to_working = f'UPDATE steps SET status = \'{step.StepStatus.working.value}\', epoch = {time.time()} WHERE id IN ({", ".join("?" * len(steps))})'
        cur.execute(sql_set_to_working, steps)
        conn.commit()

    return steps


def reset_tag_usage():
    global tag_usage
    tag_usage = {}


# Function to decrement tag usage every second
@pipe_debug.timeit
def decrement_tag_usage():
    global tag_usage
    while True:
        time.sleep(1)  # Sleep for 1 second

        with tag_lock:
            for tag in list(tag_usage.keys()):  # Use list() to create a copy of keys for safe iteration
                tag_usage[tag] = max(0, tag_usage[tag] - 1)


@pipe_debug.timeit
def receive(conn):
    data = b''
    while not data.endswith(b'[-_-]'):
        v = conn.recv(1024)
        data += v
    return data[:-5]


@pipe_debug.timeit
def send(conn, data):
    conn.sendall(data+b'[-_-]')


# def request_steps(scopes: list[str], host: str = 'localhost', port: int = 65432):
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.connect((host, port))
#         send(s, b'get-steps')
#         send(s, json_parser.dumps(scopes))
#         receive(s)
#         send(s, b'ok')
#         data = receive(s)
#         return json_parser.loads(data)
#
#
# def request_done(step_id: str, host: str = 'localhost', port: int = 65432):
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.connect((host, port))
#         send(s, b'done')
#         receive(s)
#         send(s, step_id.encode())
#         receive(s)
#
#
# def request_pending(step_id: str, host: str = 'localhost', port: int = 65432):
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.connect((host, port))
#         send(s, b'pending')
#         receive(s)
#         send(s, step_id.encode())
#         receive(s)
#
#
# def request_cancel(step_id: str, host: str = 'localhost', port: int = 65432):
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.connect((host, port))
#         send(s, b'cancel')
#         receive(s)
#         send(s, step_id.encode())
#         receive(s)
#
#
# def request_reset(step_id: str, host: str = 'localhost', port: int = 65432):
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.connect((host, port))
#         send(s, b'reset')
#         receive(s)
#         send(s, step_id.encode())
#         receive(s)
#
#
# def request_error(step_id: str, msg: str, trace: str, host: str = 'localhost', port: int = 65432):
#     with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
#         s.connect((host, port))
#         send(s, b'error')
#         receive(s)
#         send(s, step_id.encode())
#         receive(s)
#         send(s, msg.encode())
#         receive(s)
#         send(s, trace.encode())
#         receive(s)


def handle_client(connection):
    """
    Handles communication with a client.

    Args:
        connection (socket.socket): The client connection.

    Receives data from the client, processes it, and sends a response back.
    """
    def ok():
        send(connection, b'ok')
    with connection:
        data = receive(connection)
        if not data:
            return
        method, data = data.split(PIPELINE_SPLIT_TOKEN)
        method = method.decode()
        # print('received', method, data)
        if method.lower() == 'get-steps':
            scopes = json_parser.loads(data)  # (receive(connection))
            steps = get_steps(scopes)
            send(connection, json_parser.dumps(steps))
        elif method.lower() == 'done':
            pass
            # step_id = data.decode()
            # _done(step_id)
            # ok()
        elif method.lower() == 'pending':
            pass
            # step_id = data.decode()
            # _pending(step_id)
            # ok()
        elif method.lower() == 'cancel':
            pass
            # step_id = data.decode()
            # _cancel(step_id)
            # ok()
        elif method.lower() == 'reset':
            pass
            # step_id = data.decode()
            # _reset(step_id)
            # ok()
        elif method.lower() == 'error':
            pass
            # values = json_parser.loads(data)
            # step_id = values['step_id']
            # msg = values['msg']
            # trace = values['trace']
            # _error(step_id, msg, trace)
            # ok()
        else:
            response = "Unknown method."
            connection.sendall(response.encode())
    if method.lower() == 'done':
        step_id = data.decode()
        _done(step_id)
    elif method.lower() == 'pending':
        step_id = data.decode()
        _pending(step_id)
    elif method.lower() == 'cancel':
        step_id = data.decode()
        _cancel(step_id)
    elif method.lower() == 'reset':
        step_id = data.decode()
        _reset(step_id)
    elif method.lower() == 'error':
        values = json_parser.loads(data)
        step_id = values['step_id']
        msg = values['msg']
        trace = values['trace']
        _error(step_id, msg, trace)


def server_worker():
    """
    Worker thread function to process connections from the queue.

    Continuously fetches connections from the queue and processes them.
    Exits when a sentinel value (None) is received.
    """

    while True:
        connection = connection_queue.get()
        # t = time.time()
        if connection is None:
            # Sentinel received, exit the loop
            break
        try:
            handle_client(connection)
        finally:
            connection_queue.task_done()
        # pipe_debug.counter('server_worker', time.time() - t, pipe_debug.DEBUG_TABLE)


def server(host='0.0.0.0', port=65432):
    """
    Starts the server and listens for incoming connections.

    Args:
        host (str): The hostname or IP address to bind the server to.
        port (int): The port number to bind the server to.

    Accepts incoming connections and puts them into the connection queue.
    """
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.bind((host, port))
        s.listen()
        print(f"Server listening on {host}:{port}")
        while True:
            connection, addr = s.accept()
            connection.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            # print(f"Connected by {addr}")
            connection_queue.put(connection)


def main():
    """
    Main function to start the server and worker thread.

    Handles keyboard interruption to shut down the server gracefully.
    """
    load_db()

    # Start a thread for decrementing tag usage
    decrement_thread = threading.Thread(target=decrement_tag_usage)
    # decrement_thread.daemon = True  # Daemonize the thread to stop with the main program
    decrement_thread.start()

    # Start the worker thread
    worker_thread = threading.Thread(target=server_worker)
    # worker_thread.daemon = True  # Daemonize the thread to stop with the main program
    worker_thread.start()

    try:
        # Start the server
        server(PIPELINE_HOST, PIPELINE_PORT)
    except KeyboardInterrupt:
        print("Server shutting down...")
    finally:
        # Send a sentinel value to the queue to stop the worker
        connection_queue.put(None)
        worker_thread.join()


if __name__ == "__main__":
    main()


