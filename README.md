# Pipeline

Pipeline is an asynchronous ETL (Extract, Transform, Load) system that uses a custom scripting language to run code across multiple servers, one step at a time. It's designed for efficient handling of large-scale data processing tasks, particularly those involving APIs with long wait times or I/O-heavy workloads.

## Table of Contents
- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Supported Languages](#supported-languages)
- [Configuration](#configuration)
- [Usage](#usage)
- [Learn by Example](#learn-by-example)
- [Performance](#performance) <!--- - [Contributing](#contributing) -->
- [Future of Pipeline](#plans)
- [License](#license)

## Features
- Asynchronous execution of code across multiple servers
- Custom scripting language for defining ETL pipelines
- Support for Python, SQLite3, and PostgreSQL
- Efficient handling of APIs with long wait times
- Optimized for I/O-heavy workloads
- Scalable architecture for processing large amounts of data

## Installation
1. Clone the repository: `git clone https://github.com/yourusername/pipeline.git
cd pipeline`
2. Install required packages: `pip install -r requirements.txt`
3. (Optional) Build Cython files: `python build.py` (This can give a 3x performance boost)
4. (Optional) Configure PostgreSQL settings in the `.env` file.

## Quick Start
1. Run the demo server: `python demo.py`
2. In a separate terminal, run the example uploading code: `python example.py`

## Supported Languages
- Python
- SQLite3
- PostgreSQL

## Configuration
* Setup at least 4 servers on a private network (they can be small, you can technically run all these on one server like `demo.py` does but that's not recommended)
* Create a server running `python bucket.py` or something like `python -c "import c_bucket;c_bucket.main()"` 
* Create a server running `python pipeline.py` or something like `python -c "import c_pipeline;c_pipeline.main()"` 
* Create a server running `python worker.py` or something like `python -c "import c_worker;c_worker.main()"` 
* Edit the `.env` on each server to access the private ip. Change `PIPE_WORKER_HOST` to refer to the server running `pipeline.py` on server running `worker.py` and change `BUCKET_CLIENT_HOST` to refer to the server running `bucket.py` on both the `worker.py` server and the `pipeline.py` server
* Add "worker" servers until desired speed
* Create a server with private and public network access and use this to run `pipeline.upload_pipe_code_from_file` or `pipeline.upload_pipe_code` uploading the script to the server to be run.
* All workers must also have the files necessary to run your code, pip installs and all


* (Optionally) The `PIPE_WORKER_SUBPROCESS_JOBS` value within the `.env` file can be set to `true` or `false`(really anything but true). This configuration lets you run python code in a subprocess or within the "worker" script. Setting it to false gives a very slight performance increase, but requires you restart the server every time you make a change to your project.


## Usage

Pipeline uses a custom scripting language to define ETL processes. Here's how to use it:

### Basic Structure

A Pipeline script consists of steps and pipes. Each step defines a task, and pipes determine the order of execution.

```python
# Step definition
step_name:
    language
    function_or_table_name
    source_file_or_code

# Pipe definition
pipe_name = step1 | step2 | step3

# Execution
pipe_name()
```

### Supported Languages

- python: For Python code
- sqlite3: For SQLite queries
- postgres: For PostgreSQL queries

### Scopes and Priorities

Use scopes and priorities to control execution:

```python
$ production  # Set default scope


step_name:
    python
    !9  # Set priority (higher numbers run first within their scope)
    $ testing     # Set a lower priority scope
    function_name
    source_file
```

### Writing Code Directly

For short snippets, you can write code directly in the script:

```python
step_name:
    sqlite3
    table_name
    `
    SELECT * FROM table_name
    WHERE condition = 'value'
    `
```

### Defining Pipes

Pipes determine the order of step execution:

```python
single_pipe = | step1  # or `step1 |`
normal_pipe = step1 | step2 | step3
```

### Executing Pipes

There are two ways to execute pipes:

#### Single call

```python
pipe1()
result1 = pipe2()
result2 = pipe3(result1)
pipe4(result2)

pipe5(result1, result2)

# incorrect --> `pipe3(pipe2())`  #  this syntax is currently not supported
# also incorrect, they must be on one line as of now:
# `pipe3(
#   result1
# )`
```

#### Looped execution

```python
for item in pipe1():
    pipe2(item)
# incorrect --> `for item in pipe1(result):`  # syntax not supported for now
```

### Running Your Pipeline

- Save your pipeline script as a .pipe file.
- Use the Pipeline API to upload and run your script:
```python
# example.py
import pipeline

pipeline.upload_pipe_code_from_file('your_script.pipe')
```


## Learn by Example

```python
# the default scope is set to `production-small` for all steps (imports)
# setting scopes is how you make new steps with errors
# not slow down your servers by setting them to a lower scope.
# And/or how you handle processes that either require and do not require big machines to run
$ production-small

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
    $ testing-small  # <-- "scope" for a single step. A lower scope will be given less priority over higher scopes. See PIPE_WORKER_SCOPES in `.env` file
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

## this one's just to show postgres as well
#manipulate_data_again:
#    postgres
#    another_table
#    `
#select
#    *,
#    case
#        when spend = 0
#        then 0.0
#        else sales / spend
#    end AS roas
#from another_table
#`

upload:
    python
    upload_to_db
    example.py


# these are pipes and what will tell the server what order to run the steps
# and also transfer the returned  data between steps
# each step will be run individually and could be run on a different computer each time
accounts_pipe = | accounts  # single pipes currently need a `|` before or behind the value
api_pipe = request | status | download | manipulate_data | upload


# currently there are only two syntax's for "running" pipes.
# either by itself:
# pipe()
#
# or in a loop:
# for value in pipe1():
#     pipe2(value)

# # Another Example:
# v = pipe(accounts_pipe)  # <-- single call
# pipe2(v)

# right not you cannot pass arguments within the pipe being used for the for loop.
# in this case `accounts_pipe()` cannot be `accounts_pipe(some_value)`
for account in accounts_pipe():
    api_pipe(account)
```

## Performance
Pipeline is specifically designed to handle I/O-heavy workloads efficiently. It excels in scenarios such as:

- Making numerous API calls, especially to services with long processing times
- Handling large-scale data transfers between different systems
- Concurrent database operations

For instance, Pipeline is currently being used by an agency to request 30,000 reports daily from the Amazon Ads API, resulting in at least 90,000 API calls per day. This process, which includes pushing data into a PostgreSQL server with over 600 GB of data, is completed within a few hours(adding more workers could make this alot faster). The system's efficiency allows for this level of performance at a cost of under $100, including database expenses, actually the servers requesting the data are about $25.

The asynchronous nature of Pipeline makes it particularly suited for APIs like Amazon Ads, where there are significant wait times between requesting a report and its availability for download. Traditional synchronous ETL processes struggle with such APIs, especially for agencies with numerous profiles.

## Plans

If this projects sees some love, or I just find more free time, I'd like to support more languages. Even compiled languages such as `rust`, `go` and `c++`. Allowing teams that write different languages to work on the same program.

Turning this project into a pip package.

I want to rewrite this in rust for performance.


<!---
your comment goes here
and here

## Contributing
[Contributing guidelines]
-->

## License
* MIT License