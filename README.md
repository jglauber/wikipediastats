# Welcome to StatSpEdia

A tool written in Python 3.13 utilizing the async aiohttp package to grab and process data from Wikimedia using the server sent events (SSE) protocol.

All data is stored as individual documents in a local mongodb database.

## Prerequisites

Prior to installing the python package, please install mongodb community edition on your machine using the instructions here: [mongodb installation guide](https://www.mongodb.com/docs/manual/installation/)

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

By default, logs will be printed to the console and stored in a folder logs/ at the root directory.

A sample log output is as follows:

```bash
2025-06-08 14:25:13,787 - statspedia.wiki_stream - DEBUG - Buffer will be cleared when chunk completes object
2025-06-08 14:25:31,036 - statspedia.wiki_stream - DEBUG - HTTP chunk does not contain full object.
2025-06-08 14:25:31,036 - statspedia.wiki_stream - DEBUG - Buffer will be cleared when chunk completes object
2025-06-08 14:25:37,384 - statspedia.wiki_stream - DEBUG - Wiki Edit List Count is 74. Clearing and Saving to MongoDB
2025-06-08 14:25:37,387 - statspedia.wiki_stream - DEBUG - A new deep copy of Wiki Edit List was created successfully
2025-06-08 14:25:37,393 - statspedia.wiki_stream - INFO - Wiki Edit List written to latest_edits collection in MongoDB
2025-06-08 14:25:37,394 - statspedia.wiki_stream - DEBUG - Wiki Edit List succesfully cleared.
2025-06-08 14:25:37,395 - statspedia.wiki_stream - DEBUG - Program started at: 2025-06-08 01:42:55.524709+00:00
2025-06-08 14:25:37,395 - statspedia.wiki_stream - DEBUG - Current hour: 2025-06-08 21:00:00+00:00
```

### Data Schema and Basic Queries

Every server sent event from the English wikipedia is saved as a document in mongodb in a database named wiki_stream under the collection latest_changes. Every hour, the program will summarize the previous hours data in the same database in a collection named statistics. Each of these collections may be queried using the shell commands of mongosh or using the python driver [pymongo](https://pypi.org/project/pymongo/).

The schema for the latest_changes documents is:

```json
[
  {
    "_id": "ObjectId()",
    "$schema": "/mediawiki/recentchange/1.0.0",
    "meta": {
      "uri": "string",
      "request_id": "string",
      "id": "string",
      "dt": "ISODate()",
      "domain": "en.wikipedia.org",
      "stream": "mediawiki.recentchange",
      "topic": "eqiad.mediawiki.recentchange",
      "partition": "int",
      "offset": "Long()"
    },
    "id": "int",
    "type": "edit",
    "namespace": "int",
    "title": "string",
    "title_url": "string",
    "comment": "string",
    "timestamp": "int",
    "user": "string",
    "bot": "bool",
    "notify_url": "string",
    "minor": "bool",
    "length": { "old": "int", "new": "int" },
    "revision": { "old": "int", "new": "int" },
    "server_url": "https://en.wikipedia.org",
    "server_name": "en.wikipedia.org",
    "server_script_path": "/w",
    "wiki": "enwiki",
    "parsedcomment": "string",
    "bytes_change": "int"
  }
]
```

The schema for statistics is:

```json
{
    "most_data_added": {},
    "most_data_removed": {},
    "top_editors": {},
    "top_editors_bots": {},
    "all_editors": {},
    "all_editors_bots": {},
    "top_edited_articles": {},
    "all_edited_articles": {},
    "num_edited_articles": "int",
    "num_editors": "int",
    "num_editors_bots": "int",
    "num_edits": "int",
    "bytes_added": "int",
    "bytes_removed": "int",
    "total_bytes_change": "int",
    "timestamp": "ISODate()"

}
```

Below are some simple examples for how to perform queries of the database using pymongo. For more information on queries please see the documentation here: 

```python
from pymongo import MongoClient
from pymongo.cursor import Cursor
from datetime import datetime, timezone

client = MongoClient(host='mongodb://127.0.0.1',port=27017)
db = client.wiki_stream
collection1 = db.statistics
collection2 = db.latest_changes


def create_cur(field: str, collection) -> Cursor:
    cur = collection.find({field: {'$exists': 1}},{'_id': 0, field: 1})
    return cur

def edit_count_by_user(cur: Cursor, field: str):
    user_edit_dict = {}

    for i in cur:
        user_generator = ((k,v) for (k,v) in i[field].items())
        for user,edit_count in user_generator:
            try:
                user_edit_dict[user] += edit_count
            except KeyError:
                user_edit_dict[user] = edit_count
    
    total_unique_editors = len(user_edit_dict.keys())

    sorted_user_edit_dict = dict(sorted(user_edit_dict.items(),
                                        key=lambda item: item[1],
                                        reverse=True)[0:10])

    return sorted_user_edit_dict, total_unique_editors


def edit_count_by_document(cur: Cursor, field: str):
    document_edit_dict = {}
    count = 0
    for i in cur:
        document_title = i[field]
        try:
            document_edit_dict[document_title] += 1
        except KeyError:
            document_edit_dict[document_title] = 1
        count += 1
    
    total_documents_edited = len(document_edit_dict.keys())

    sorted_document_edit_dict = dict(sorted(document_edit_dict.items(),
                                        key=lambda item: item[1],
                                        reverse=True)[0:10])

    return sorted_document_edit_dict, total_documents_edited, count



def sum_across_all_stats(cur: Cursor, field: str):
    total = 0

    for i in cur:
        total += i[field]
    
    return total

cur = create_cur('all_editors', collection1)
users, num_unique_users = edit_count_by_user(cur,'all_editors')
print(f"Top Editors (Human) All Time: {users}")
print(f"Total Editors (Human) All Time: {num_unique_users}")

cur2 = create_cur('all_editors_bots', collection1)
users, num_unique_users_bots = edit_count_by_user(cur2,'all_editors_bots')
print(f"Top Editors (Bots) All Time: {users}")
print(f"Total Editors (Bots) All Time: {num_unique_users_bots}")

cur3 = create_cur('num_edits', collection1)
num_edits = sum_across_all_stats(cur3,'num_edits')
print(f"Total Edits All Time {num_edits}")

cur4 = create_cur('bytes_added', collection1)
bytes_added = sum_across_all_stats(cur4,'bytes_added')
print(f"Total MB Added All Time {bytes_added/1e6}")

cur5 = create_cur('bytes_removed', collection1)
bytes_removed = sum_across_all_stats(cur5,'bytes_removed')
print(f"Total MB Removed All Time {bytes_removed/1e6}")

cur6 = create_cur('total_bytes_change', collection1)
bytes_change = sum_across_all_stats(cur6,'total_bytes_change')
print(f"Total MB Change All Time {bytes_change/1e6}")

cur7 = create_cur('timestamp', collection1)
for i in cur7:
    print(f"Data recording started on: {i['timestamp']}")
    break

cur8 = create_cur('title', collection2)
top_docs_edited, total_docs_edited, count = edit_count_by_document(cur8,'title')
print(f"Most Edited Docs: {top_docs_edited}")
print(f"Total Edited Docs: {total_docs_edited}")
print(count)

cur9 = create_cur('all_edited_articles', collection1)
articles, total_articles = edit_count_by_user(cur9,'all_edited_articles')
print(f"Most Edited Docs: {articles}")
print(f"Total Edited Docs: {total_articles}")
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
