import time
import sqlite3
import uuid
import random
import json
import sys

import pipeline
import sqlite3_helper
import postgres
import step

import pipe_debug
counter = pipe_debug.counter


def accounts(*args):
    """
    a function that returns a list of accounts

    Returns:
        a list of accounts
    """
    # time.sleep(random.randint(1, 5))

    return [
        {'name': 'mr. business', 'id': 123},
        {'name': 'mrs. business', 'id': 456},
        {'name': 'sr. business', 'id': 789}
    ]


def request_report(data: dict) -> dict:
    """
    a function that requests a report

    Args:
        data: the data to include in the report

    Returns:
        the data with a report_id added
    """
    # time.sleep(random.randint(1, 5))

    report_id = f'{uuid.uuid4()}'
    return {**data, 'report_id': report_id}


def get_status(data: dict) -> tuple[step.StepStatus, dict] | dict:
    """
    a function that returns the status of a report

    Args:
        data: the data containing the report_id

    Returns:
        the status of the report
    """
    # time.sleep(random.randint(1, 5))

    if data.get('status') == 'failed':
        return step.StepStatus.cancel, data

    if data.get('status') == 'report deleted':
        return step.StepStatus.reset, data

    if counter(data['report_id']) < random.randint(3, 25):
        return step.StepStatus.pending, data

    return data


def get_report(data: dict) -> list[dict]:
    """
    a function that returns a report

    Args:
        data: the data containing the report_id

    Returns:
        a list of dictionaries representing the report
    """
    # time.sleep(random.randint(1, 5))

    return [{**data, 'sales': 100 * (13 % i), 'spend': 50 * (9 % i)}
            for i in range(1, 50)]


def upload_to_db(data: list[dict]) -> None:
    """
    a function that uploads data to a database

    Args:
        data: the data to upload
    """
    # time.sleep(random.randint(1, 5))

    db = sqlite3_helper.Sqlite3('test.db')
    db.upload_table('test', data)
    counter('done1')


def main():
    try:
        db = sqlite3_helper.Sqlite3('test.db')
        t = db.download_table('test')
    except sqlite3.OperationalError:
        t = []

    l1 = len(t)

    number_of_jobs = 1

    print('the table test currently has', l1, 'rows')
    try:
        for i in range(number_of_jobs):
            pipeline.upload_pipe_code_from_file('example.pipe')
    except ConnectionRefusedError:
        raise ConnectionRefusedError(f'please start the pipeline first by running "{sys.executable} demo.py"')

    print(f'waiting waiting until all tasks finish')
    while len(accounts()) * number_of_jobs > (c := counter('done1', 0)):
        # print(f'waiting, {len(accounts()) - c} accounts left of {len(accounts())}')
        time.sleep(.1)

    db = sqlite3_helper.Sqlite3('test.db')
    t = db.download_table('test')
    print('the table test now has', len(t), 'rows. A difference of', len(t) - l1)
    # print('the new rows are:', t[l1:])

    counter_table = [row for row in db.download_table('counter') if row['id'] != 'done']
    print('take a look that the counter', json.dumps(counter_table, indent=4), 'is updated')

    # t = '\n\t'.join([f'{row["count"]:0.2f} | {row["id"]}' for row in pg.download_table(sql='select * from debug order by count desc')])
    # print(f'debug:\n\t{t}')

    db.query('delete from counter where 1=1;')


if __name__ == '__main__':
    main()


