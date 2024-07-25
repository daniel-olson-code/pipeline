import os
import asyncio
import traceback
import socket
import time
import subprocess
import shlex
import sys
import enum
from typing import Dict, List, Union, Any, Optional

import step
import pipeline
import bucket
import json_parser
import pipe_debug

import asyncio_pool

try:
    import dotenv
    dotenv.load_dotenv()
except ModuleNotFoundError:
    pass

# pipeline.load_db()

WORKER_HOST = os.environ.get('PIPE_WORKER_HOST', 'localhost')
WORKER_PORT = int(os.environ.get('PIPE_WORKER_PORT', 65432))
PIPE_WORKER_SUBPROCESS_JOBS = os.environ.get('PIPE_WORKER_SUBPROCESS_JOBS') == 'true'

bucket_client = bucket.Client()

JOB_CMD = f'{sys.executable} -c "import worker;worker.job()"'


class HandleStatus(enum.Enum):
    success = 'success'
    pending = 'pending'
    almost = 'almost'
    none = 'none'


def job(step_id: str | None = None) -> None:
    # t = time.time()
    if step_id:
        _step = pipeline.get_step(step_id)
    else:
        _step = pipeline.get_step(os.environ['STEP_ID'])
    print('handling', _step.name)
    try:
        args = [pipeline.get_data(_id) for _id in _step.parents]
        r: step.Result = _step.run(*args)
        pipeline.set_data(_step.id, r.data)

        if r.status == step.StepStatus.success:
            request_done(_step.id)
        elif r.status == step.StepStatus.pending:
            request_pending(_step.id)
        elif r.status == step.StepStatus.reset:
            request_reset(_step.id)
        elif r.status == step.StepStatus.cancel:
            request_cancel(_step.id)
        else:
            raise Exception('Invalid step status')
        # pipe_debug.counter(f'[step].{_step.name}', time.time() - t, pipe_debug.DEBUG_TABLE)
    except Exception as e:
        print(' - Error - ')
        print(str(e))
        traceback.print_exc()
        request_error(
            _step.id,
            str(e),
            f'{traceback.format_exc()}'
        )


async def run(step_id: str | None = None) -> None:
    if PIPE_WORKER_SUBPROCESS_JOBS != 'true':
        return job(step_id)
    env = {**os.environ, 'STEP_ID': step_id}
    p = await asyncio.create_subprocess_shell(JOB_CMD, env=env)
    await p.wait()
    # env = {**os.environ, 'STEP_ID': step_id}
    # p = subprocess.Popen(
    #     shlex.split(JOB_CMD),
    #     env=env,
    #     # stdout=subprocess.PIPE,
    #     # stderr=subprocess.PIPE,
    # )
    # p.wait()
    #
    # # stdout, stderr = p.communicate()
    # # print(stdout, stderr)
    # # return p.returncode == 0


@pipe_debug.timeit
async def handle(scopes):
    steps = request_steps(scopes)

    # print('steps', steps)
    if not steps:
        # yield HandleStatus.none
        # return
        return False

    # # Example 1
    # tasks = [asyncio.create_task(run(s)) for s in steps]
    #
    # done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    #
    # while len(pending) > 0:
    #     print("Get more results in 2 seconds:")
    #     _done, pending = await asyncio.wait(pending, timeout=.5)
    #     done += _done
    #     if len(done) / len(steps) > .5:
    #         yield HandleStatus.almost
    #
    #     yield HandleStatus.pending
    #
    # yield HandleStatus.success

    # # Example 2
    # await asyncio.gather(*[run(s) for s in steps])

    # Example 3
    async with asyncio_pool.AioPool(size=50) as pool:
        for s in steps:
            await pool.spawn(run(s))

    return True


async def work():
    _scopes: str = os.environ.get('PIPE_WORKER_SCOPES', 'default')
    scopes: List = _scopes.split(',')
    print('scopes', scopes)

    last_loop_had_steps = True
    while True:
        # async for status in handle(scopes, loop):
        #     if status == HandleStatus.almost or status == HandleStatus.success:
        #         break
        #     elif status == HandleStatus.none:
        #         await asyncio.sleep(1.)
        if await handle(scopes):
            last_loop_had_steps = True
        else:
            if last_loop_had_steps:
                print('waiting..')
            last_loop_had_steps = False
            await asyncio.sleep(1.)

        # await asyncio.sleep(.01)


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


@pipe_debug.timeit
def request_steps(scopes: list[str]):
    global WORKER_HOST, WORKER_PORT
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        data = b'get-steps' + pipeline.PIPELINE_SPLIT_TOKEN + json_parser.dumps(scopes)
        send(s, data)
        data = receive(s)
        return json_parser.loads(data)


@pipe_debug.timeit
def request_done(step_id: str):
    global WORKER_HOST, WORKER_PORT
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        data = b'done' + pipeline.PIPELINE_SPLIT_TOKEN + step_id.encode()
        send(s, data)
        # receive(s)


@pipe_debug.timeit
def request_pending(step_id: str):
    global WORKER_HOST, WORKER_PORT
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        data = b'pending' + pipeline.PIPELINE_SPLIT_TOKEN + step_id.encode()
        send(s, data)
        # receive(s)


@pipe_debug.timeit
def request_cancel(step_id: str):
    global WORKER_HOST, WORKER_PORT
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        data = b'cancel' + pipeline.PIPELINE_SPLIT_TOKEN + step_id.encode()
        send(s, data)
        # receive(s)


@pipe_debug.timeit
def request_reset(step_id: str):
    global WORKER_HOST, WORKER_PORT
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        data = b'reset' + pipeline.PIPELINE_SPLIT_TOKEN + step_id.encode()
        send(s, data)
        # receive(s)


@pipe_debug.timeit
def request_error(step_id: str, msg: str, trace: str):
    global WORKER_HOST, WORKER_PORT
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        s.connect((WORKER_HOST, WORKER_PORT))
        data = b'error' + pipeline.PIPELINE_SPLIT_TOKEN + json_parser.dumps({'step_id': step_id, 'msg': msg, 'trace': trace})
        send(s, data)
        # receive(s)


def main():
    asyncio.run(work())


if __name__ == '__main__':
    main()

