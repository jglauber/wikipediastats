import aiohttp
import asyncio
from aiohttp import client_exceptions
import json
import time
import re
from re import Match
import pickle
import base64
import os
from pymongo import AsyncMongoClient
import copy
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field, asdict
import logging
import collections.abc as abc
from .errors import (
    PrimaryStreamError,
    BackupStreamError
    )

# initiate logger
logger = logging.getLogger(__name__)

# create logs folder
os.makedirs('logs', exist_ok=True)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
file_handler = logging.FileHandler('logs/application.log', encoding='utf-8')
formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
stream_handler.setLevel(logging.DEBUG)
file_handler.setFormatter(formatter)
file_handler.setLevel(logging.DEBUG)
logger.addHandler(stream_handler)
logger.addHandler(file_handler)


@dataclass
class WikiStatistics:
    """Class to keep track of high level statistics"""
    most_data_added: dict = field(default_factory=dict)
    most_data_removed: dict = field(default_factory=dict)
    top_editors: dict = field(default_factory=dict)
    top_editors_bots: dict = field(default_factory=dict)
    all_editors: dict = field(default_factory=dict)
    all_editors_bots: dict = field(default_factory=dict)
    top_edited_articles: dict = field(default_factory=dict)
    all_edited_articles: dict = field(default_factory=dict)
    num_edited_articles: int = 0
    num_editors: int = 0
    num_editors_bots: int = 0
    num_edits: int = 0
    bytes_added: int = 0
    bytes_removed: int = 0
    
    def total_bytes_change(self) -> int:
        return self.bytes_added - self.bytes_removed

class WikiStream:
    """
    The WikiStream Class serves as the primary entrypoint and allows users to
    run the stream which generates wiki_edit_list files

    Attributes
    ----------
    url : str
        The url that contains the recent changes.

    file_name : str

    Methods
    -------
    stream()
        Runs the main stream of Wikimedia changes.
    """

    def __init__(self):
        self.url = "https://stream.wikimedia.org/v2/stream/recentchange"
        self.timeout = 5
        self.start_time = datetime.now(tz=timezone.utc)
        self.current_hour = _round_dt_nearest_hour(self.start_time)
        self._wiki_list_lock = asyncio.Lock()
        self._backup_wiki_list_lock = asyncio.Lock()
        self.wiki_edit_list = []
        self._backup_wiki_edit_list = []
        self.mongo_client = AsyncMongoClient(host="mongodb://127.0.0.1", port=27017)
        self.mongo_db = self.mongo_client.wiki_stream
        self.mongo_collection = self.mongo_db.latest_changes
        self.server_drop_event = asyncio.Event()
        self.primary_stream_running = asyncio.Event()
        self.cancel_secondary_stream = asyncio.Event()
        self.start_secondary_stream = asyncio.Event()
        self.secondary_stream_await_timer = asyncio.Event()
        self.last_item_id = 0
        self.first_item_id = 0
        self._backup_stream_task = asyncio.create_task(self._backup_stream())


    async def _wiki_edits_generator(self) -> abc.AsyncGenerator[list[dict]]:
        """
        A generator function that yields wikipedia edits as a list of dicts.
        """

        async with aiohttp.ClientSession(raise_for_status=True) as session:
            async with session.request(method='GET', url=self.url, timeout=aiohttp.ClientTimeout(total=None, sock_connect=10, sock_read=10)) as response:
                # create a buffer
                buffer = b""
                logger.debug(f'HTTP Connection Status Code: {response.status}')
                try:
                    async for data, end_of_http_chunk in response.content.iter_chunks():
                        buffer += data
                        if end_of_http_chunk:
                            result = buffer.decode(errors="replace")
                            if result[-1] == '\n':
                                # if last element is new line character, clear buffer
                                buffer = b""
                                result_list = _convert_buffer_to_list(result)

                                yield result_list
                            
                            else:
                                logger.debug('HTTP chunk does not contain full object.')
                                logger.debug('Buffer will be cleared when chunk completes object')
                                continue

                except client_exceptions.ClientPayloadError as e:
                    logger.error(f'Client Payload Error: {e}')
                    logger.warning('Restarting Wiki Edit Stream')
                    raise
                except client_exceptions.SocketTimeoutError as e:
                    logger.critical(f'Socket Timeout Error: {e}')
                    logger.warning('Restarting Wiki Edit Stream')
                    raise
                except client_exceptions.ClientConnectorDNSError:
                    logger.critical('Host DNS Server Error')


    async def _primary_stream_handler(self):
        """
        Handle the primary stream and
        cancel and restart when errors occur.
        """

        while True:
            try:
                task1 = asyncio.create_task(self._primary_stream(self._backup_stream_task))
                await task1
            
            except PrimaryStreamError:
                continue


    async def _primary_stream(self, task: asyncio.Task):
        """
        Primary wikipedia edit events stream
        """

        start_up_flag = True
        try:
            async for latest_edits in self._wiki_edits_generator():
                if len(latest_edits) > 0:
                    self.last_item_id = latest_edits[-1]['id']
                    if start_up_flag:
                        self.first_item_id = latest_edits[0]['id']
                        self.server_drop_event.clear()
                        self.primary_stream_running.set()
                        self.start_secondary_stream.clear()
                        if not task.done():
                            task.cancel()
                        while True:
                            try:
                                await task
                            except asyncio.CancelledError:
                                break
                        self.secondary_stream_await_timer.set()
                        task = asyncio.create_task(self._backup_stream())
                        self._backup_stream_task = task
                        logger.info("Backup stream reset")
                        start_up_flag = False

                    async with self._wiki_list_lock:
                        self.wiki_edit_list += latest_edits
                    
        except client_exceptions.ClientPayloadError:
            msg = "Primary stream dropped by server."
            logger.info(msg)
            self.primary_stream_running.clear()
            self.server_drop_event.set()
            raise PrimaryStreamError(msg)
        
        except client_exceptions.SocketTimeoutError:
            msg = "Primary stream socket timeout error."
            logger.info(msg)
            self.primary_stream_running.clear()
            self.server_drop_event.set()
            raise PrimaryStreamError(msg)

        except client_exceptions.ClientResponseError as e:
            msg = f"Primary stream encountered client response error: {e}"
            logger.info(msg)
            self.primary_stream_running.clear()
            self.server_drop_event.set()
            raise PrimaryStreamError(msg)
            

    async def _recover_lost_data(self):
        while True:
            await self.server_drop_event.wait()
            logger.info("The server timed out and dropped client.")

            last_item_id = copy.deepcopy(self.last_item_id)

            await self.primary_stream_running.wait()
            logger.info("The connection to server was restored.")

            first_item_id = copy.deepcopy(self.first_item_id)
            
            logger.debug(f"The last id logged before server issue: {last_item_id}")
            logger.debug(f"The first id logged after server recovered: {first_item_id}")

            temp_missing_items_list = []
            for _,item in enumerate(self._backup_wiki_edit_list):
                if item.get('id') > last_item_id and item.get('id') < first_item_id:
                    temp_missing_items_list.append(item)
            
            num_recovered_items = len(temp_missing_items_list)
            
            logger.debug(f"There were {num_recovered_items} items recovered.")

            # add missing data to wiki_edit_list:
            async with self._wiki_list_lock:
                self.wiki_edit_list += temp_missing_items_list

            # clear backup list
            async with self._backup_wiki_list_lock:
                self._backup_wiki_edit_list.clear()
        

    async def _schedule_backup_stream(self, timeout: int = 120):
        while True:
            await self.secondary_stream_await_timer.wait()
            logger.debug(f"Scheduling backup stream to start in {timeout} seconds.")
            await asyncio.sleep(timeout)
            self.secondary_stream_await_timer.clear()
            self.start_secondary_stream.set()


    async def _backup_stream(self):
        """
        A redundant method to gather wikipedia edit events.
        """

        while True:
            await self.start_secondary_stream.wait()
            logger.debug("Backup stream running again.")
            try:
                async for latest_edits in self._wiki_edits_generator():
                    async with self._backup_wiki_list_lock:
                        self._backup_wiki_edit_list += latest_edits
            except client_exceptions.ClientPayloadError:
                msg = "Backup stream dropped by server."
                logger.info(msg)
                continue
            except client_exceptions.ClientResponseError as e:
                msg = f"Backup stream encountered client response error: {e}"
                logger.info(msg)
                continue
            except client_exceptions.SocketTimeoutError as e:
                msg = f"Backup stream socket timeout error: {e}"
                logger.info(msg)
                continue


    async def _write_list_to_db(self):
        """
        Write the wiki list to the db.
        """

        while True:
            await asyncio.sleep(30)
            # check if a new hour has arrived
            await self._summarize_stats_hourly()
            wiki_edit_list_count = len(self.wiki_edit_list)
            logger.debug('Wiki Edit List Count is %s. Clearing and Saving to MongoDB', wiki_edit_list_count)
            await self.clear_list_and_save()

            debug_list = [
                ('Program started at: %s', self.start_time),
                ('Current hour: %s', self.current_hour),
            ]

            for debug_str, debug_var in debug_list:
                logger.debug(debug_str,debug_var)


    async def _monitor_backup_list_size(self):
        while True:
            await asyncio.sleep(30)
            num_backup_items = len(self._backup_wiki_edit_list)
            # if primary stream remains stable for a long time,
            # ensure that backup edit list is cleared periodically.
            if num_backup_items > 5000:
                async with self._backup_wiki_list_lock:
                    self._backup_wiki_edit_list.clear()
            # print(f"Number of Items in Backup List: {num_backup_items}")        


    async def _summarize_stats_hourly(self):
        """
        A function that summarizes the last hour of data and stores as
        a document in mongodb
        """

        count = 0
        next_hour = self.current_hour + timedelta(hours=1)
        for item in self.wiki_edit_list:
            if next_hour != _round_dt_nearest_hour(item['meta']['dt']):
                count += 1
                break

        if len(self.wiki_edit_list) > 0 and count == 0:
            logger.info('All Items in Wiki Edit List have shifted to new hour.')
            # ensure data is written to the database
            await self.clear_list_and_save()
            last_hour_data = self.mongo_collection.find({ 'meta.dt': { '$gte': self.current_hour, '$lt': next_hour} })
            last_hour_data = await last_hour_data.to_list()

            logger.info('There were %s edits in the last hour.', len(last_hour_data))
                    
            # store hourly stats in mongodb
            wiki_statistics = _create_stats_object(last_hour_data)
            logger.info('New stats object created for last hour of data')
            total_bytes_change = wiki_statistics.total_bytes_change()
            logger.info('The total bytes change for the last hour was %s', total_bytes_change)
            stats_dict = asdict(wiki_statistics)
            stats_dict['total_bytes_change'] = total_bytes_change
            stats_dict['timestamp'] = self.current_hour

            stats_collection = self.mongo_db['statistics']
            await stats_collection.insert_one(stats_dict)
            logger.info('Statistics document added to MongoDB for the last hour of data')

            # replace current hour with next hour
            self.current_hour = next_hour


    def encode(self) -> str:
        """
        A method to pickle and b64 encode python object as a string.
        """
        # pickle the list
        pickled_list = pickle.dumps(self.wiki_edit_list)
        # b64 encode
        data = base64.b64encode(pickled_list).decode()
        return data

    async def stream(self):
        start = self.start_time.timestamp()
        try:
            async with asyncio.TaskGroup() as tg:
                task1 = tg.create_task(self._primary_stream_handler())
                task2 = tg.create_task(self._recover_lost_data())
                task3 = tg.create_task(self._write_list_to_db())
                task4 = tg.create_task(self._schedule_backup_stream())
                task5 = tg.create_task(self._monitor_backup_list_size())

        except asyncio.CancelledError:
            while True:
                if (task1.cancelled() and
                    task2.cancelled() and
                    task3.cancelled() and
                    task4.cancelled() and
                    task5.cancelled()):
                    # await self._write_buf_to_list()
                    # await self.clear_list_and_save()
                    logger.info('All tasks cancelled by user')
                    end = datetime.now(tz=timezone.utc).timestamp()
                    total_time = elapsed_time(start, end)
                    logger.info('Total program runtime: %s', total_time)
                    break

    async def clear_list_and_save(self):
        # write to mongodb
        wiki_edit_list = self.wiki_edit_list
        if len(wiki_edit_list) > 0:
            wiki_edit_list_copy = copy.deepcopy(wiki_edit_list)
            logger.debug('A new deep copy of Wiki Edit List was created successfully')
            inserted_ids = 0
            while inserted_ids == 0:
                result = await self.mongo_collection.insert_many(wiki_edit_list_copy)
                inserted_ids = result.inserted_ids
            logger.info('Wiki Edit List written to latest_edits collection in MongoDB')

        async with self._wiki_list_lock:
            # clear the wiki_edit_list
            self.wiki_edit_list.clear()
            logger.debug('Wiki Edit List succesfully cleared.')


def _convert_buffer_to_list(string_buf) -> list:
    """
    Convert the buffer to a python list.
    """

    # remove new line separators
    string_buf = re.sub(r"\n", "", string_buf)

    # remove ok comment
    string_buf = re.sub(r":ok", "", string_buf)

    # remove event: message line
    string_buf = re.sub(r"event: message", "", string_buf)

    # remove id category
    string_buf = re.sub(r"id: \[[\s\S]+?]", "", string_buf)

    # remove data string to expose underlying object
    string_buf = re.sub(r"data: ", "", string_buf)
    
    # add commas to separate multiple entries
    index = string_buf.find('{"$schema"')
    if index > -1:
        string_buf = string_buf[index:]
        string_buf = string_buf.replace("}{", "},{")

    # fix issues with double vs. single quotes
    string_buf = fix_comments(string_buf)

    # ensure backslashes are properly escaped
    string_buf = re.sub(r"(?<=[^\\])\\(?=[^\\ubfnrt\/])", r"\\\\", string_buf)
    string_buf = re.sub(r"event: messagedata: ", ",", string_buf)

    # add list brackets
    string_buf = "[" + string_buf + "]"

    latest_edit_list = []

    if string_buf == "[]":
        return latest_edit_list

    i = 0
    while True:
        try:
            latest_edit_list = json.loads(string_buf)
            break
        except json.JSONDecodeError as e:
            logger.warning('JSON Decode Error in Latest Edit List')
            if i > 100:
                logger.error('Unhandled Decoder Issue: %s', e.msg)
                latest_edit_list = []
                with open(
                    f"unhandled_decoder_issue_{time.strftime('%Y-%m-%d %H:%M:%S')}.json",
                    "w",
                ) as f:
                    f.write(e.msg + "\n")
                    f.write(f"column: {e.colno}" + "\n")
                    f.write(f"char: {e.pos}" + "\n")
                    f.write(string_buf)
                    f.close()
                break

            elif e.msg == "Invalid \\escape":
                if i == 0:
                    logger.warning("Starting Loop to Remove Invalid Escape Characters")
                logger.warning("Attempt %s: %s at %s", i, e.msg, e.pos)
                string_buf = string_buf[: e.pos] + string_buf[e.pos + 1 :]

            i += 1

    # filter list for edits only.
    new_list = []
    for item in latest_edit_list:
        try:
            if (item["type"] == "edit" or item["type"] == "new") and item[
                "meta"
            ]["domain"] == "en.wikipedia.org":
                new = item["length"].get("new", 0)
                old = item["length"].get("old", 0)
                difference = new - old

                # add bytes change to latest_edit_list dicts
                item['bytes_change'] = difference

                # convert dt field to a datetime object
                item['meta']['dt'] = datetime.strptime(item['meta']['dt'], '%Y-%m-%dT%H:%M:%SZ').replace(tzinfo=timezone.utc)

                new_list.append(item)
        except KeyError as e:
            if item['meta']['domain'] == 'canary':
                logger.warning('Canary Event on Wikistreams')
            else:
                logger.warning('Item in Latest Edit List Missing Key: %s', e)

    return new_list


def decode(b64_string: str) -> list:
    """
    A function to decode a b64 encoded string back to python list.
    """
    data = base64.b64decode(b64_string.encode())
    py_list = pickle.loads(data)
    return py_list


def check_size_bytes(string: str) -> int:
    """
    A function to check the size of a string in bytes
    """
    return len(string.encode())


def elapsed_time(start: float, end: float) -> str:
    """
    A function to calculate the elapsed time in string format \
    using the start and end times as inputs.
    """
    elapsed_time = end - start
    days = elapsed_time // (24 * 3600)
    hours = (elapsed_time % (24 * 3600)) // 3600
    mins = ((elapsed_time % (24 * 3600)) % 3600) // 60
    secs = round(((elapsed_time % (24 * 3600)) % 3600) % 60, 1)
    return f"Elapsed Time: {days} days {hours} hours {mins} mins {secs} secs"


def fix_comments(input_string: str) -> str:
    """
    A function that takes an input string and ensures
    proper use of quotations
    """

    fixed_string = re.sub(r"\u200e", "", input_string)
    fixed_string = re.sub(r"\u200f", "", fixed_string)
    fixed_string = re.sub(
        r'(?<="parsedcomment":)[\s\S]+?(?=},{"\$schema")', _replace_quot, fixed_string
    )
    fixed_string = re.sub(
        r'(?<="comment":)[\s\S]+?(?=,"timestamp")', _replace_quot, fixed_string
    )
    fixed_string = re.sub(
        r'(?<="log_action_comment":)[\s\S]+?(?=,"server_url")',
        _replace_quot,
        fixed_string,
    )
    fixed_string = re.sub(
        r'(?<="title":)[\s\S]+?(?=,"title_url")',
        _replace_quot,
        fixed_string,
    )
    fixed_string = re.sub(
        r'(?<="target":)[\s\S]+?(?=,"noredir")',
        _replace_quot,
        fixed_string,
    )
    fixed_string = re.sub(
        r'(?<="user":)[\s\S]+?(?=,"bot")',
        _replace_quot,
        fixed_string,
    )
    index = fixed_string.rfind('"parsedcomment":') + 16
    if index > 15:
        fixed_string = (
            fixed_string[:index] + _replace_quot(input_string=fixed_string[index:-1]) + "}"
        )
    return fixed_string


def _replace_quot(matchobj: Match = None, input_string: str = ""):
    """
    A helper function to remove double quotes.
    """
    if matchobj is not None:
        substring = matchobj.group(0)
    else:
        substring = input_string
    text = substring.replace('"', "'")

    return f'"{text}"'


# def _convert_dt_string_to_dt(dt: str, format: str = '%Y-%m-%dT%H:%M:%SZ') -> datetime:
#     new_dt_obj = datetime.strptime(dt, format).replace(tzinfo=timezone.utc)
#     return new_dt_obj

def _round_dt_nearest_hour(dt: datetime) -> datetime:
    """
    A function that rounds down a datetime to nearest hour
    """
    current_hour_ts = dt.timestamp()
    # remove remainder to round down to nearest hour
    current_hour_rounded = current_hour_ts - (current_hour_ts % 3600)
    current_hour_rounded_dt = datetime.fromtimestamp(current_hour_rounded, tz=timezone.utc)
    return current_hour_rounded_dt

# db.latest_changes.find({'meta.dt':{$gte:'2025-05-26T13:00:00Z', $lt:'2025-05-26T14:00:00Z'}}).count()

def _create_stats_object(mongodb_data: list) -> WikiStatistics:
    """
    A function that takes a list of mongodb data from the the database
    and returns high level statistics.
    """

    wiki_statistics = WikiStatistics()
    editors = Editors(all_edits=mongodb_data)
    wiki_statistics.num_edits = editors.num_edits
    wiki_statistics.top_editors = editors.top_editors_human(100)
    wiki_statistics.top_editors_bots = editors.top_editors_bot(100)
    wiki_statistics.num_editors = editors.total_editors_human()
    wiki_statistics.num_editors_bots = editors.total_editors_bot()
    wiki_statistics.most_data_added = editors.most_data_added
    wiki_statistics.most_data_removed = editors.most_data_removed
    wiki_statistics.bytes_added = editors.bytes_added
    wiki_statistics.bytes_removed = editors.bytes_removed
    wiki_statistics.all_editors = editors.human
    wiki_statistics.all_editors_bots = editors.bot
    wiki_statistics.top_edited_articles = editors.top_edited_articles(100)
    wiki_statistics.all_edited_articles = editors.edits_by_title
    wiki_statistics.num_edited_articles = editors.total_docs_edited()

    return wiki_statistics
    
class Editors:
    """
    Class to keep track of unique editors and number of edits
    """

    def __init__(self, all_edits: list[dict]) -> None:
        self.human = {}
        self.bot = {}
        self.most_data_added = {}
        self.most_data_removed = {}
        self.num_edits = len(all_edits)
        self.bytes_added = 0
        self.bytes_removed = 0
        self.edits_by_title = {}
        if self.num_edits > 0:
            for item in all_edits:
                user = item.get("user")
                bot = item.get("bot", False)
                title = item.get("title")

                # collate edits for each article title

                if title in self.edits_by_title.keys():
                    self.edits_by_title[title] += 1
                else:
                    self.edits_by_title[title] = 1

                # collate edits by humans
                if not bot:
                    if user in self.human.keys():
                        self.human[user] += 1
                    else:
                        self.human[user] = 1
                
                # collate edits by bots
                if bot:
                    if user in self.bot.keys():
                        self.bot[user] += 1
                    else:
                        self.bot[user] = 1

                if item['bytes_change'] > 0:
                    self.bytes_added += item['bytes_change']

                    # add most data added edit to class attribute
                    most_data_added = self.most_data_added
                    if item['bytes_change'] > most_data_added.get('bytes_change', 0):
                        self.most_data_added = item

                if item['bytes_change'] < 0:
                    self.bytes_removed += -1 * item['bytes_change']

                    # add most data removed to class attribute
                    most_data_removed = self.most_data_removed
                    if -1 * item['bytes_change'] > most_data_removed.get('bytes_change', 0):
                        self.most_data_removed = item

    def total_editors_human(self) -> int:
        return len(self.human.keys())

    def total_editors_bot(self) -> int:
        return len(self.bot.keys())
    
    def total_docs_edited(self) -> int:
        return len(self.edits_by_title.keys())
    
    def top_editors_human(self, num_editors: int) -> dict:
        if len(self.human.items()) < num_editors:
            return dict(
                sorted(
                    self.human.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
            )
        else:
            return dict(
                sorted(
                    self.human.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[0:num_editors]
            )
        
    def top_editors_bot(self, num_editors: int) -> dict:
        if len(self.bot.items()) < num_editors:
            return dict(
                sorted(
                    self.bot.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
            )
        else:
            return dict(
                sorted(
                    self.bot.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[0:num_editors]
            )
    
    def top_edited_articles(self, num_articles: int) -> dict:
        if len(self.edits_by_title.items()) < num_articles:
            return dict(
                sorted(
                    self.edits_by_title.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )
            )
        else:
            return dict(
                sorted(
                    self.edits_by_title.items(),
                    key=lambda item: item[1],
                    reverse=True,
                )[0:num_articles]
            )