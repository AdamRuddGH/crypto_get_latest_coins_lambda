"""
Shared utilities. Mostly for time
"""

import arrow
import json
import re

def datetime_now():
    """
    returns UTC now time
    add .format('YYYY-MM-DD HH:mm:ss ZZ') to convert to ISO
    add .format()
    """
    return arrow.utcnow()

def epoch_to_timestamp(epoch_string):
    """
    converts data from epoch to athena friendly timestamp
    """
    
    epoch_string_clean = str(int(epoch_string/1000) ) #remove milliseconds
    try:
        formatted_timestamp = arrow.get(epoch_string_clean,"X").format("YYYY-MM-DD HH:mm:ss")
    except:
        formatted_timestamp = datetime_now().format("YYYY-MM-DD HH:mm:ss")
    return formatted_timestamp

def iso8601_to_timestamp(input_iso8601_timestamp):
    """
    takes an iso8601 timestamp to timestamp
    """
    return arrow.get(input_iso8601_timestamp).format("YYYY-MM-DD HH:mm:ss")

def dict_to_jsonl(dict_input):
    """
    takes a dict object and turns it into a jsonl doc
    """

    jsonl_contents = json.dumps(dict_input)
    jsonl_contents = re.sub(f"\n","",jsonl_contents)

    return jsonl_contents


def list_of_dicts_to_jsonl(list_input):
    """
    takes a list of dict objects and turns it into a jsonl doc
    """

    jsonl_contents = ""
    for each_entry in list_input:
        if len(jsonl_contents) == 0:
            jsonl_contents = json.dumps(each_entry)
        else:
            jsonl_contents = jsonl_contents + "\n" + json.dumps(each_entry)

    return jsonl_contents


def date_to_partition_path(input_date_string):
    """
    will take a date string and return a partition path
    eg. 
    "2021-01-30" -> "year=2021/month=01/day=30"
    """
    if isinstance(input_date_string, str):
        raw_date = arrow.get(input_date_string)
        year = raw_date.format("YYYY")
        month = raw_date.format("MM")
        day = raw_date.format("DD")

        partition_path = f"year={year}/month={month}/day={day}"

        return partition_path

    else:
        raise TypeError("date supplied to date_to_partition_path function is not a string")

