import aiohttp
import asyncio
from aiohttp import client_exceptions
import json
import io
import time
import re
from re import Match
import pickle
import base64
import os
from pymongo import AsyncMongoClient
import copy
from datetime import datetime, timezone
from dataclasses import dataclass, field, asdict
import logging

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
        self._buf = io.StringIO()
        self._lock = asyncio.Lock()
        self._wiki_list_lock = asyncio.Lock()
        self.wiki_edit_list = []
        self.mongo_client = AsyncMongoClient(host="mongodb://127.0.0.1", port=27017)
        self.mongo_db = self.mongo_client.wiki_stream
        self.mongo_collection = self.mongo_db.latest_changes

    async def _wiki_edit_stream(self):
        """
        An async function to stream wikimedia recent changes.
        """

        async with aiohttp.ClientSession() as session:
            async with session.get(self.url) as response:
                # create a buffer
                buffer = b""
                try:
                    async for data, end_of_http_chunk in response.content.iter_chunks():
                        buffer += data
                        if end_of_http_chunk:
                            result = buffer.decode(errors="ignore")

                            # clear buffer
                            buffer = b""

                            async with self._lock:
                                self._buf.write(result)
                except asyncio.TimeoutError:
                    logger.error('Timeout Error')
                    logger.warning('Restarting Wiki Edit Stream')
                    return await self._wiki_edit_stream()
                except client_exceptions.ClientPayloadError:
                    logger.error('Client Payload Error')
                    logger.warning('Restarting Wiki Edit Stream')
                    return await self._wiki_edit_stream()
                except client_exceptions.ClientConnectorDNSError:
                    logger.critical('Host DNS Server Error')

    async def _write_buf_to_list(self):
        """
        Write the buffer io.StringIO to a list.
        """
        string_buf = self._buf.getvalue()
        async with self._lock:
            self._buf.seek(0)
            self._buf.truncate()

        if string_buf != "":
            string_buf = re.sub(r":ok\n\n", "", string_buf)
            string_buf = re.sub(r"event: message", "", string_buf)
            string_buf = re.sub(r"data: ", "", string_buf)
            string_buf = re.sub(r"(?<=[\n])id: \[[\s\S]+?]", "", string_buf)
            string_buf = re.sub(r"\n", "", string_buf)
            index = string_buf.find('{"$schema"')
            string_buf = string_buf[index:]
            string_buf = string_buf.replace("}{", "},{")
            string_buf = fix_comments(string_buf)
            string_buf = re.sub(r"(?<=[^\\])\\(?=[^\\ubfnrt\/])", r"\\\\", string_buf)
            string_buf = re.sub(r"event: messagedata: ", ",", string_buf)
            string_buf = "[" + string_buf + "]"
            latest_edit_list = []

            i = 0
            while True:
                try:
                    latest_edit_list = json.loads(string_buf)
                    logger.info('String Buffer Loaded as JSON to Latest Edit List')
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

            latest_edit_list = new_list

            # update wiki_edit_list
            async with self._wiki_list_lock:
                self.wiki_edit_list += latest_edit_list

            await self._summarize_stats_hourly()

            wiki_edit_list_count = len(self.wiki_edit_list)
            if wiki_edit_list_count > 30:
                logger.debug('Wiki Edit List Count is %s. Clearing and Saving to MongoDB', wiki_edit_list_count)
                await self.clear_list_and_save()

            debug_list = [
                ('Program started at: %s', self.start_time),
                ('Current hour: %s', self.current_hour),
            ]

            for debug_str, debug_var in debug_list:
                logger.debug(debug_str,debug_var)


    async def _loop_buf_to_list(self):
        """
        A function to manage the loop of writing the buffer to a list
        """
        while True:
            await asyncio.sleep(self.timeout)
            await self._write_buf_to_list()

    async def _summarize_stats_hourly(self):
        """
        A function that summarizes the last hour of data and stores as
        a document in mongodb
        """

        count = 0
        for item in self.wiki_edit_list:
            new_current_hour = _round_dt_nearest_hour(item['meta']['dt'])
            if new_current_hour == self.current_hour:
                count += 1

        if count == 0:
            # ensure data is written to the database
            await self.clear_list_and_save()
            last_hour_data = self.mongo_collection.find({ 'meta.dt': { '$gte': self.current_hour, '$lt': new_current_hour} })
            last_hour_data = await last_hour_data.to_list()

            logger.info('There were %s edits in the last hour.', len(last_hour_data))
                
            # create a new mongodb collection to store this hour of data.
            # new_collection = self.mongo_db[self.current_hour]
            # await new_collection.insert_many([data for data in last_hour_data])
            # logger.debug('Data successfully written to MongoDB')
            # await self.mongo_collection.delete_many({ 'meta.dt': { '$gte': self.current_hour, '$lt': new_current_hour } })
            # logger.debug('Data moved from latest_edits collection to %s', self.current_hour)     
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

            # replace current hour with new hour
            self.current_hour = new_current_hour


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
                task1 = tg.create_task(self._wiki_edit_stream())
                task2 = tg.create_task(self._loop_buf_to_list())
                # task3 = tg.create_task(self._summarize_stats_hourly())
                # task3 = tg.create_task(self._check_list_size_bytes())
        except asyncio.CancelledError:
            if (task1.cancelled() and
                task2.cancelled()):
                await self._write_buf_to_list()
                await self.clear_list_and_save()
                logger.info('All tasks cancelled by user')
                end = datetime.now(tz=timezone.utc).timestamp()
                total_time = elapsed_time(start, end)
                logger.info('Total program runtime: %s', total_time)

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
        if self.num_edits > 0:
            for item in all_edits:
                user = item.get("user")
                bot = item.get("bot", False)
                if not bot:
                    if user in self.human.keys():
                        self.human[user] += 1
                    else:
                        self.human[user] = 1
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