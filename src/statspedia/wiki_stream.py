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


class WikiStream():
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
        self.url = 'https://stream.wikimedia.org/v2/stream/recentchange'
        self.file_name = 'test.json'
        self.timeout = 5
        self._buf = io.StringIO()
        self._lock = asyncio.Lock()
        self._wiki_list_lock = asyncio.Lock()
        self.wiki_edit_list = []
        self.bytes_added = 0
        self.bytes_removed = 0
        self.editors = {"human": {},
                        "bot": {}}
        self.top_10_editors = []
        self.top_10_editor_bots = []
        self.longest_edit = {"bytes_added": 0,
                             "edit_data": {}}
        self.most_data_removed = {"bytes_removed": 0,
                                  "edit_data": {}}

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
                            result = buffer.decode(errors='ignore')

                            # clear buffer
                            buffer = b""

                            # remove event, comments, and ascii formatting.
                            replacements = [('event: message', ''),
                                            (':ok',''),
                                            ('‎','')]


                            for old, new in replacements:
                                result = result.replace(old,new)
                            
                            # remove message id.
                            # result = re.sub(r"id:[\s\S]+?]","",result)

                            # remove new line separator.
                            result = re.sub(r"\n","",result)

                            # remove data property.
                            result = re.sub(r"data: ","",result)

                            async with self._lock:
                                self._buf.write(result)
                except asyncio.TimeoutError:
                    print("Timeout Error")
                    return await self._wiki_edit_stream()
                except client_exceptions.ClientPayloadError:
                    print("Client Payload Error")
                    return await self._wiki_edit_stream()
                except client_exceptions.ClientConnectorDNSError:
                    print("Host DNS Server Error")

    async def _write_buf_to_list(self):
        """
        Write the buffer io.StringIO to a list.
        """
        string_buf = self._buf.getvalue()
        async with self._lock:
            self._buf.seek(0)
            self._buf.truncate()
        
        if string_buf != "":
            string_buf = re.sub(r"id: \[[\s\S]+?]","",string_buf)
            string_buf = string_buf.replace('}{','},{')
            string_buf = fix_comments(string_buf)
            string_buf = re.sub(r"(?<=[^\\])\\(?=[^\\ubfnrt\"\/])",r"\\\\",string_buf)
            string_buf = '[' + string_buf + ']'
            latest_edit_list = []

            try:        
                latest_edit_list = json.loads(string_buf)
            except json.JSONDecodeError as e:
                with open(f"unhandled_decoder_issue_{time.strftime('%Y-%m-%d %H:%M:%S')}.json", 'w') as f:
                    f.write(e.msg+'\n')
                    f.write(f"column: {e.colno}"+'\n')
                    f.write(f"char: {e.pos}"+'\n')
                    f.write(string_buf)
                    f.close()
                latest_edit_list = [] 

            new_list = []
            for item in latest_edit_list:
                try:
                    if ((item['type'] == 'edit' or item['type'] == 'new')
                        and item['meta']['domain'] == 'en.wikipedia.org'):
                        new_list.append(item)
                except KeyError as e:
                    print(f"ERROR: Missing Key {e}")
                    print(json.dumps(item, ensure_ascii=False, indent=4))
            
            latest_edit_list = new_list

            # filter list for edits only.
            async with self._wiki_list_lock:
                self.wiki_edit_list += latest_edit_list

            # calculate bytes changes on Wikipedia
            bytes_added,bytes_removed = self.calc_bytes(latest_edit_list)

            # check size in bytes.
            size_bytes = check_size_bytes(self.encode())
            size_Mbytes = round(size_bytes/1e6,2)

            if size_Mbytes > 5: # change to 50 in prod.
                await self.clear_list_and_save()

            # determine top 10 editors
            self.track_edits(latest_edit_list)
            # bots
            if len(self.editors['bot'].items()) < 10:
                self.top_10_editor_bots = dict(sorted(self.editors['bot'].items(), key=lambda item: item[1], reverse=True))
            else:
                self.top_10_editor_bots = dict(sorted(self.editors['bot'].items(), key=lambda item: item[1], reverse=True)[0:10])
            
            # human
            if len(self.editors['human'].items()) < 10:
                self.top_10_editors = dict(sorted(self.editors['human'].items(), key=lambda item: item[1], reverse=True))
            else:
                self.top_10_editors = dict(sorted(self.editors['human'].items(), key=lambda item: item[1], reverse=True)[0:10])
            

            # print status
            os.system('clear')
            print(f"There are {len(self.wiki_edit_list)} items in the list.")
            print(f"Wiki Edit List Size: {size_Mbytes} MB")
            print(f"Bytes Added: {bytes_added}")
            print(f"Bytes Removed: {bytes_removed}")
            print(f"Total Bytes Change: {bytes_added - bytes_removed}")
            print(f"Top 10 Editors: \n{json.dumps(self.top_10_editors, ensure_ascii=False, indent=4)}\n")
            print(f"Top 10 Editors (Bots): \n{json.dumps(self.top_10_editor_bots, ensure_ascii=False, indent=4)}\n")
            print(f"Most Data Removed: \n{json.dumps(self.most_data_removed, ensure_ascii=False, indent=4)}\n")
            print(f"Most Data Added: \n{json.dumps(self.longest_edit, ensure_ascii=False, indent=4)}\n")

    async def _loop_buf_to_list(self):
        """
        A function to manage the loop of writing the buffer to a list
        """
        while True:
            await asyncio.sleep(self.timeout)
            await self._write_buf_to_list()

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
        start = time.time()
        try:
            async with asyncio.TaskGroup() as tg:
                task1 = tg.create_task(self._wiki_edit_stream())
                task2 = tg.create_task(self._loop_buf_to_list())
                # task3 = tg.create_task(self._check_list_size_bytes())
        except asyncio.CancelledError:
            if task1.cancelled() and task2.cancelled():
                await self._write_buf_to_list()
                with open(self.file_name, 'w') as f:
                    json.dump(self.wiki_edit_list,f, ensure_ascii=False, indent=4)
                    f.close()
                print("All tasks cancelled.")
                end = time.time()
                total_time = elapsed_time(start,end)
                print(total_time)

    async def clear_list_and_save(self):
        # write to file
        with open(f"wiki_edit_list_{time.strftime('%Y-%m-%d %H:%M:%S')}.json", 'w') as f:
            json.dump(self.wiki_edit_list,f, ensure_ascii=False, indent=4)
            f.close()

        async with self._wiki_list_lock:
            # clear the wiki_edit_list
            self.wiki_edit_list.clear()

    def calc_bytes(self, latest_edit_list: list) -> tuple:
        bytes_added = self.bytes_added
        bytes_removed = self.bytes_removed
        for item in latest_edit_list:
            try:
                new = item['length']['new']
            except KeyError:
                new = 0
            try:
                old = item['length']['old']
            except KeyError:
                old = 0
            
            difference = new - old
            if difference > 0:
                self.bytes_added += difference

                # add longest edit to class attribute
                longest_edit = self.longest_edit['bytes_added']
                if difference > longest_edit:
                    self.longest_edit['bytes_added'] = difference
                    self.longest_edit['edit_data'] = item

            if difference < 0:
                self.bytes_removed += -1*difference

                # add most data removed to class attribute
                most_data_removed = self.most_data_removed['bytes_removed']
                if -1*difference > most_data_removed:
                    self.most_data_removed['bytes_removed'] = -1*difference
                    self.most_data_removed['edit_data'] = item
            
        return (bytes_added,bytes_removed)
    
    def track_edits(self, latest_edit_list: list) -> dict:
        editors = self.editors
        for item in latest_edit_list:
            try:
                bot = item['bot']
                user = item['user']
                if not bot:
                    if user in editors['human'].keys():
                        editors['human'][user] += 1
                    else:
                        editors['human'][user] = 1
                if bot:
                    if user in editors['bot'].keys():
                        editors['bot'][user] += 1
                    else:
                        editors['bot'][user] = 1
            except KeyError:
                if user in editors['human'].keys():
                    editors['human'][user] += 1
                else:
                    editors['human'][user] = 1


        return editors
        

    
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
    days = elapsed_time // (24*3600)
    hours = (elapsed_time % (24*3600)) // 3600
    mins = ((elapsed_time % (24*3600)) % 3600) // 60
    secs = round(((elapsed_time % (24*3600)) % 3600) % 60,1)
    return f"Elapsed Time: {days} days {hours} hours {mins} mins {secs} secs"

def fix_comments(input_string: str) -> str:
    """
    A function that takes an input string and ensures
    proper use of quotations
    """

    fixed_string = re.sub(r'(?<="parsedcomment":)[\s\S]+?(?=},{"\$schema")',_replace_quot,input_string)
    fixed_string = re.sub(r'(?<="comment":)[\s\S]+?(?=,"timestamp")',_replace_quot,fixed_string)
    fixed_string = re.sub(r'(?<="log_action_comment":)[\s\S]+?(?=,"server_url")',_replace_quot,fixed_string)
    index = fixed_string.rfind('"parsedcomment":') + 16
    fixed_string = fixed_string[:index] + _replace_quot(input_string=fixed_string[index:-1]) + '}'
    return fixed_string    

def _replace_quot(matchobj: Match = None, input_string: str = ''):
    """
    A helper function to remove double quotes.
    """
    if matchobj is not None:
        substring = matchobj.group(0)
    else:
        substring = input_string
    text = substring.replace('"',"'")

    return f'"{text}"'
