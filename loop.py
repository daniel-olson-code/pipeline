"""
This module provides functionality for evaluating pipes and handling loops in a custom pipeline language.
"""

from typing import Callable

import step_definition
import pipe
import pipe_util
import pipe_interpreter

def pipe_eval(index: int, txt: str, variables: dict):
    """
    Evaluate a pipe expression and execute its steps.

    Args:
        index (int): The current line index in the pipeline code.
        txt (str): The pipe expression to evaluate.
        variables (dict): A dictionary containing pipeline variables and functions.

    Returns:
        The result of executing the pipe's steps.

    Raises:
        SyntaxError: If the function call is invalid or unknown.
        TypeError: If the called object is not a pipe.Pipe instance.
    """
    if txt.count('(') == 1 and txt.endswith(')'):
        name, args = txt[:-1].split('(')
        name = pipe_util.trim(name)
        if name not in variables:
            raise SyntaxError(f'Line {index+1}: call to unknown function \'{name}\'')
        _pipe: pipe.Pipe = variables[name]
        if not isinstance(_pipe, pipe.Pipe):
            raise TypeError(f'Line {index+1}: \'{name}\' is not a pipe.Pipe. It\'s \'{type(_pipe)}\'')
        steps = _pipe.create_steps(index, variables, args, pipe_util.json_copy(_pipe.kwargs))
        data = []
        for step in steps:
            data = step.run(data).data
        for step in steps:
            del variables['__steps__'][step.id]
            variables['__starters__'].remove(step.id)
        return data

def check_loop(
        index: int,
        line: str,
        lines: list[str],
        variables: dict,
        read_line: Callable[[int], None],
        set_index: Callable[[int], None]
) -> bool:
    """
    Check and process a loop construct in the pipeline code.

    Args:
        index (int): The current line index in the pipeline code.
        line (str): The current line of code being processed.
        lines (list[str]): All lines of the pipeline code.
        variables (dict): A dictionary containing pipeline variables and functions.
        read_line (Callable[[int], None]): A function to process a specific line of code.
        set_index (Callable[[int], None]): A function to set the current line index.

    Returns:
        bool: True if a loop was found and processed, False otherwise.

    Raises:
        TypeError: If the loop variable is not a list.
    """
    if (line.strip().startswith('for ')
            and line.count(' in ') == 1
            and line.strip().endswith(':')):
        before, after = line.split(' in ')
        var_name = pipe_util.trim(before.strip()[3:])
        loop_var = pipe_util.trim(after.strip()[:-1])

        loop_var = pipe_eval(index, loop_var, variables)

        if not isinstance(loop_var, list):
            raise TypeError(f'Line {index + 1}: Loop variable must be a list')

        def get_indentation(text: str):
            """
            Calculate the indentation level of a line of code.

            Args:
                text (str): A line of code.

            Returns:
                int: The indentation level.
            """
            indentation = 0
            while text.startswith(pipe_interpreter.TAB):
                indentation += 1
                text = text[len(pipe_interpreter.TAB):]
            return indentation

        indentation = get_indentation(line)
        next_line = index + 1
        future_lines = []
        for j in range(index + 1, len(lines)):
            if get_indentation(lines[j]) <= indentation:
                break
            future_lines.append(j)
            next_line = j + 1

        for _i, value in enumerate(loop_var):
            _code = f'def main(*args):\n    return {value}'
            sd = step_definition.StepDefinition(f'def_{var_name}_{_i}', 'PYTHON', 'main', '', _code, True)
            sd.scope = variables['__scope__']
            variables[f'def_{var_name}_{_i}'] = sd
            p = pipe.Pipe(var_name, f'def_{var_name}_{_i}')
            variables[var_name] = p.create_steps(index, variables, '')
            for future_line in future_lines:
                read_line(future_line)

        set_index(next_line - 1)
        return True
    return False

# from typing import Callable
#
# import step_definition
# import pipe
# import pipe_util
# import pipe_interpreter
#
#
# def pipe_eval(index: int, txt: str, variables: dict):
#     if txt.count('(') == 1 and txt.endswith(')'):
#         name, args = txt[:-1].split('(')
#         name = pipe_util.trim(name)
#         if name not in variables:
#             raise SyntaxError(f'Line {index+1}: call to unknown function \'{name}\'')
#         _pipe: pipe.Pipe = variables[name]
#         if not isinstance(_pipe, pipe.Pipe):
#             raise TypeError(f'Line {index+1}: \'{name}\' is not a pipe.Pipe. It\'s \'{type(_pipe)}\'')
#         steps = _pipe.create_steps(index, variables, args, pipe_util.json_copy(_pipe.kwargs))
#         data = []
#         for step in steps:
#             # step: step.Step
#             data = step.run(data).data
#         for step in steps:
#             del variables['__steps__'][step.id]
#             variables['__starters__'].remove(step.id)
#         return data
#
#
# def check_loop(
#         index: int,
#         line: str,
#         lines: list[str],
#         variables: dict,
#         read_line: Callable[[int], None],
#         set_index: Callable[[int], None]
# ) -> bool:
#     if (line.strip().startswith('for ')
#             and line.count(' in ') == 1
#             and line.strip().endswith(':')):
#         before, after = line.split(' in ')
#         var_name = pipe_util.trim(before.strip()[3:])
#         loop_var = pipe_util.trim(after.strip()[:-1])
#
#         loop_var = pipe_eval(index, loop_var, variables)
#
#         # if isinstance(loop_var, Call):
#         #     loop_var = loop_var.run(variables)
#         if not isinstance(loop_var, list):
#             raise TypeError(f'Line {index + 1}: Loop variable must be a list')
#
#         def get_indentation(text: str):
#             indentation = 0
#             while text.startswith(pipe_interpreter.TAB):
#                 indentation += 1
#                 text = text[len(pipe_interpreter.TAB):]
#             return indentation
#
#         indentation = get_indentation(line)
#         next_line = index + 1
#         future_lines = []
#         for j in range(index + 1, len(lines)):
#             if get_indentation(lines[j]) <= indentation:
#                 break
#             future_lines.append(j)
#             next_line = j + 1
#
#         for _i, value in enumerate(loop_var):
#             _code = f'def main(*args):\n    return {value}'
#             sd = step_definition.StepDefinition(f'def_{var_name}_{_i}', 'PYTHON', 'main', '', _code, True)
#             sd.scope = variables['__scope__']
#             variables[f'def_{var_name}_{_i}'] = sd
#             p = pipe.Pipe(var_name, f'def_{var_name}_{_i}')
#             variables[var_name] = p.create_steps(index, variables, '')  # value
#             for future_line in future_lines:
#                 read_line(future_line)
#
#         set_index(next_line - 1)
#         return True
#     return False
