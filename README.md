## Pipeline

---

To test this project run `python demo.py`
then in a separate terminal `example.py`.
`demo.py` showcases the server(s) 
and `example.py` showcases the client uploading the code to the server(s).

This project was made to create a custom ETL.
Although, I'm sure you can think of something better 
to do with it.
It allows you to run your programs asynchronously between multiple languages. 
The program is run asynchronously across as 
many servers you need or want.

You program is split into `steps`.
Each `step` is put within a `pipe`.
The `pipe` tells the server the order of the `step`s.
All `step`s are run on as many servers as you have 
running the `worker.py` or `c_worker.pyx` scripts or their `main()` function,
these servers are typically pretty small.
Below you'll find the requirements and usage.

Learn by Example:
```python
# the defailt scope is set to `production` for all steps (imports)
$ production

# step 1: `accounts`
accounts:
    python  # <-- select the language to be run. currently only python, sqlite3 and postgres are available
    accounts  # define the function or table name that will be used
    example.py  # either provide a file or write code directly using the "`" char (see below example)

request:
    python
    request_report
    example.py

status:
    python
    $ testing  # <-- "scope" a lower scope will be given less priority over higher scopes. See PIPE_WORKER_SCOPES in `.env` file
    get_status
    example.py

download:
    python
    !9  # <-- "priority" higher numbers are more important and run first within their scope.
    get_report
    example.py

manipulate_data:
    sqlite3
    some_table  # *vvvv* see below for writing code directly *vvvv*
    `
SELECT
    *,
    CASE
        WHEN sales = 0
        THEN 0.0
        ELSE spend / sales
    END AS acos
FROM some_table
`

# this one's just to show postgres as well
manipulate_data_again:
    postgres
    another_table
    `
select
    *,
    case
        when spend = 0
        then 0.0
        else sales / spend
    end AS roas
from another_table
`

upload:
    python
    upload_to_db
    example.py


# these are pipe and what will be run
# each step will be run individually and could be run on a different computer each time
accounts_pipe = | accounts  # single pipes currently need a `|` before or behind the value
api_pipe = request | status | download | manipulate_data | manipulate_data_again | upload


# currently there are only two syntax's for "running" pipes.
# either by itself: `pipe()`
# or in a loop:
# `for value in pipe1():
#     pipe2(value)

# v = pipe(accounts_pipe)  # <-- single call
# pipe2(v)

# right not you cannot pass arguments within the pipe being used for the for loop.
for account in accounts_pipe():
    api_pipe(account)
```

---

### Real World Example

I've used similar to the above code to request 30,000 reports
daily, which is at least 90,000 API calls to Amazon Ads API 
per day
and pushing that into a PostgreSQL server, 
the process is done within a few hours (this could be sped up by adding more workers, but my timeframe is 24 so that's not needed).
All for pretty cheap, 
including a PostgreSQL server with over 600 GB of data
and growing,
it's under a $100.

For those who aren't familiar with Amazon Ads API, Amazon has you request a report, 
wait a long time for the report to be done.
Check the status until it's done.
Then download the report. 
Synchronous ETL's perform very poorly for these API's 
and don't even work for accounts with 200+ profiles (profiles are like sub-accounts).


---





