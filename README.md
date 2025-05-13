# Welcome to StatSpEdia

A tool written in Python 3.13 utilizing the async aiohttp package to grab and process data from Wikimedia using the server sent events (SSE) protocol.

## Installation

To install a local copy please run:
`pip install statspedia`

## Example Usage

### Create an Instance of the WikiStream Class

```python
from statspedia import WikiStream
import asyncio

async def main():
    ws = WikiStream()
    return await ws.stream()
    
asyncio.run(main())
```

### Program Console Output

```bash
There are 34 items in the list.
Wiki Edit List Size: 0.04 MB
Bytes Added: 1775
Bytes Removed: 669
Total Bytes Change: 1106
Top 10 Editors: 
{
    "BD2412": 4,
    "Panamitsu": 4,
    "Bearian": 1,
    "Altenmann": 1,
    "PCN02WPS": 1,
    "CarlTheCoincleaner": 1,
    "Madhon335": 1,
    "Bamboofirdaus": 1,
    "Xexerss": 1,
    "Youknowmyname657": 1
}

Top 10 Editors (Bots): 
{
    "GreenC bot": 8
}

Most Data Removed: 
{
    "bytes_removed": 403,
    "edit_data": {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {
            "uri": "https://en.wikipedia.org/wiki/Basem_Abdo",
            "request_id": "6dc5cf9f-3641-45c7-8d71-7aba1ebfdef6",
            "id": "7edf7ae0-dad3-4a33-bc12-66779552448e",
            "dt": "2025-05-13T02:25:08Z",
            "domain": "en.wikipedia.org",
            "stream": "mediawiki.recentchange",
            "topic": "eqiad.mediawiki.recentchange",
            "partition": 0,
            "offset": 5582582578
        },
        "id": 1903433073,
        "type": "edit",
        "namespace": 0,
        "title": "Basem Abdo",
        "title_url": "https://en.wikipedia.org/wiki/Basem_Abdo",
        "comment": "'Deprod. Likely to be controversial. Please go to AfD.'",
        "timestamp": 1747103108,
        "user": "Bearian",
        "bot": false,
        "notify_url": "https://en.wikipedia.org/w/index.php?diff=1290144333&oldid=1290126553",
        "minor": false,
        "length": {
            "old": 5672,
            "new": 5269
        },
        "revision": {
            "old": 1290126553,
            "new": 1290144333
        },
        "server_url": "https://en.wikipedia.org",
        "server_name": "en.wikipedia.org",
        "server_script_path": "/w",
        "wiki": "enwiki",
        "parsedcomment": "'Deprod. Likely to be controversial. Please go to AfD.'"
    }
}

Most Data Added: 
{
    "bytes_added": 758,
    "edit_data": {
        "$schema": "/mediawiki/recentchange/1.0.0",
        "meta": {
            "uri": "https://en.wikipedia.org/wiki/Church_of_the_Assumption_(Nashville,_Tennessee)",
            "request_id": "e60e4d1c-961b-4512-b35e-2167046f2a1c",
            "id": "eb1cf52f-78d5-4c98-9028-8edb5ae1db83",
            "dt": "2025-05-13T02:25:19Z",
            "domain": "en.wikipedia.org",
            "stream": "mediawiki.recentchange",
            "topic": "eqiad.mediawiki.recentchange",
            "partition": 0,
            "offset": 5582582788
        },
        "id": 1903433115,
        "type": "edit",
        "namespace": 0,
        "title": "Church of the Assumption (Nashville, Tennessee)",
        "title_url": "https://en.wikipedia.org/wiki/Church_of_the_Assumption_(Nashville,_Tennessee)",
        "comment": "'/* History of the Parish in the 20th Century */ 1906 renovation which installed current pews and floor, carnival lights, and other features of church today as well as removed/modified features'",
        "timestamp": 1747103119,
        "user": "Johnnygoesmarchinghome",
        "bot": false,
        "notify_url": "https://en.wikipedia.org/w/index.php?diff=1290144350&oldid=1290141507",
        "minor": false,
        "length": {
            "old": 27414,
            "new": 28172
        },
        "revision": {
            "old": 1290141507,
            "new": 1290144350
        },
        "server_url": "https://en.wikipedia.org",
        "server_name": "en.wikipedia.org",
        "server_script_path": "/w",
        "wiki": "enwiki",
        "parsedcomment": "'<span class=\\'autocomment\\'><a href=\\'/wiki/Church_of_the_Assumption_(Nashville,_Tennessee)#History_of_the_Parish_in_the_20th_Century\\' title=\\'Church of the Assumption (Nashville, Tennessee)\\'>→<bdi dir=\\'ltr\\'>History of the Parish in the 20th Century</bdi></a>: </span> 1906 renovation which installed current pews and floor, carnival lights, and other features of church today as well as removed/modified features'"
    }
}
```

### Stopping the Program

The program may be safely stopped using `ctrl + c` which will cancel all active async tasks.

The following will be outputted to the console:

```bash
All tasks cancelled.
Elapsed Time: 0.0 days 0.0 hours 0.0 mins 18.9 secs
```

## License

MIT

## Project Status

In development.

## Authors

John Glauber

## Contact

For any questions, comments, or suggestions please reach out via email to:  
  
John Glauber  
<johnbglauber@gmail.com>
