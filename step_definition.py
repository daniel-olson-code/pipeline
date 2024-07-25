from __future__ import annotations
import enum
from typing import Any, List

import execution
import pipe_util


class Result(object):
    status: StepStatus = True
    env: dict = None
    priority: int = None
    velocity: float = None
    data: Any = None

    def __init__(self, status = None, env = None, priority = None, velocity = None, data = None,):
        self.status = status
        self.env = env or {}
        self.priority = priority
        self.velocity = velocity
        self.data = data

    @classmethod
    def from_result(cls, result):
        self = cls()
        status, env, p, v, data = result
        self.status = status
        self.env = env
        self.priority = p
        self.velocity = v
        self.data = data
        return self


class StepStatus(enum.Enum):
    success = 1
    queued = 2
    pending = 3
    cancel = 4
    reset = 5
    working = 6
    error = 7


def create_return_value(value) -> Result:
    result = None
    if isinstance(value, Result):
        return value
    if not isinstance(value, tuple):
        result = StepStatus.success, None, None, None, value
    else:  # if isinstance(value, tuple):
        if len(value) == 2:
            result = value[0], None, None, None, value[1]
        elif len(value) == 3:
            result = value[0], value[1], None, None, value[2]
            # result = value[0], value[1], None, None, None, Data({'value': value[2]})
        elif len(value) == 4:
            result = value[0], value[1], value[2], None, value[3]  # Data({'value': value[2]})
            # result = value[0], value[1], value[2], None, None, Data({'value': value[2]})
        elif len(value) == 5:
            result = value[0], value[1], value[2], value[3], value[4]  # Data({'value': value[2]})
            #  result = value[0], value[1], value[2], value[3], None, Data({'value': value[2]})
        else:
            raise ValueError('value is not a list or tuple.')
        # if len(value) == 6:
        #     result = value[0], value[1], value[2], value[3], value[4], value[5]  # Data({'value': value[2]})
        #     #  result = value[0], value[1], value[2], value[3], value[4], Data({'value': value[2]})
    # if self.caching:
    #     self.cache = result
    if result is None:
        raise ValueError('value is not a list or tuple.')
    return Result.from_result(result)


class Step(pipe_util.PipeObject):
    id: str = None
    name: str = 'empty'
    type: str = None
    code: str = None
    func: str = None
    local: str = False  # code found locally
    kwargs: dict = None
    scope: str = 'default'
    tag: str = None
    priority: int = 0
    velocity: float = None

    parents: list[str] = None
    children: List[str] = None

    def get_code(self):
        code = self.code
        if self.local:
            with open(self.code, 'r') as f:
                code = f.read()
        return code

    def run(self, *args: Any) -> Result:
        # print('stepping', self.name)
        code = self.get_code()
        if self.type == 'POSTGRESQL':
            return create_return_value(execution.run_postgres(code, self.func, *args, **self.kwargs))  # True, Data({'value': run_postgres(code, self.func, data.value, **self.kwargs)})
        if self.type == 'PYTHON':
            # done, value = run_py(code, self.func, data.value, **self.kwargs)
            return create_return_value(execution.run_py(code, self.func, *args, **self.kwargs))  # done, Data({'value': value})
        if self.type == 'SQLITE3':
            return create_return_value(execution.run_sqlite3(code, self.func, *args, **self.kwargs))  # True, Data({'value': run_sqlite3(code, self.func, data.value, **self.kwargs)})

