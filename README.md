## Pipeline.


#### Note:
The Public version of this project is still under development.

---

This project was made to create a custom ETL.
Although, I'm sure you can think of something better 
to do with it.
It allows you to run your programs asynchronously. 
The program is run asynchronously across as 
many servers you need or want.

I've used the following code to request 30,000 reports
daily and which is at least 90,000 API calls to Amazon Ads API per day
and pushing that into a PostgreSQL server.
All for pretty cheap, 
including a PostgreSQL server with over 600 GB of data
and growing,
it's under a $100.

Learn by Example:
```
{
    "env": "var",
    "this": "is a json"
}

$production

accounts:
    python 
    request_report 
    amazon_ads_api.py

request: 
    python 
    request_report 
    amazon_ads_api.py

status:
    python
    report_status
    amazon_ads_api.py

download:
    python
    download_report
    amazon_ads_api.py
    
upload:
    python
    $testing
    upload_report_to_db
    my_db.py


report_api = request | status | download | upload
accounts_pipe = | accounts


# # single use case
# report_api()


for account in accounts():
    report_api(account)


```

```
step1:
    python
    main
    `
def main(*args):
    return [
        {'a': 23, 'b': 'hello'},    
        {'a': 24, 'b': 'bye'},
    ]
`

step2:
    postgres
    _
    `
select * 
from table
where a = 24
 `
 
step3:
    python
    main
    `
def main(data):
    assert len(data) == 1
    assert data[0]['a'] == 24
    data.append({'a': 25, 'b': 'howdy'})
    return data    
`
 
 
 pipe = step1 | step2 | step3
 
 pipe()

```


