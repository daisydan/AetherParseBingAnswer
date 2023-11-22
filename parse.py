"""Script to extract web answer details from scraped pbjson."""

import argparse
import base64
import csv
import json
import logging
from typing import Any, Dict, List, Tuple
import datetime
import time

import pandas as pd

JsonDict = Dict[str, Any]
JsonList = List[JsonDict]
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s:%(name)s:%(message)s",
    handlers=[
        logging.StreamHandler(),
    ])
logger = logging.getLogger()
logger = logging.getLogger(__name__)

def decode_base64_pbjson(
        encoded_base64_pbjson: str) -> JsonDict:
    """Decode base64 encoded pbjson to JSON dictionary."""
    return json.loads(base64.b64decode(encoded_base64_pbjson))


def extract_answer_from_pbjson(
        decoded_pbjson: JsonDict, topN: int) -> JsonList:
    """Extract webanswers from pbjson."""
    answer_data_array: JsonList = decoded_pbjson.get('PropertyBag', None).get('AnswerResponseCommand', None).get('AnswerQueryResponse', None).get('AnswerDataArray', None)
    for element in answer_data_array:
        if element.get('AnswerServiceName', None) == 'MultimediaKifVideoAnswer':
            answer_data_kif_response: JsonList = element.get('AnswerDataKifResponse', None)
            web_answers: JsonList = answer_data_kif_response[0].get('results', None)
            web_answers = web_answers[:topN]
            return web_answers
    return [{}]


def extract_short_answer_from_pbjson(
        decoded_pbjson: JsonDict, topN: int) -> JsonList:
    """Extract webanswers from pbjson."""
    answer_data_array: JsonList = decoded_pbjson.get('PropertyBag', None).get('AnswerResponseCommand', None).get('AnswerQueryResponse', None).get('AnswerDataArray', None)
    for element in answer_data_array:
        if element.get('AnswerServiceName', None) == 'MultimediaShortVideoAnswer':
            answer_data_kif_response: JsonList = element.get('AnswerDataKifResponse', None)
            web_answers: JsonList = answer_data_kif_response[0].get('webResults', None)
            web_answers = web_answers[:topN]
            return web_answers
    return [{}]


def extract_webanswer_parts(
        encoded_base64_pbjson: str, videoType: int, topN: int) -> List[str]:
    """Extract specified columns from webanswers."""
    try:
        decoded_pbjson: JsonDict = decode_base64_pbjson(encoded_base64_pbjson)
        if videoType == 0:    # bing answer
            extracted_webanswer_array: JsonList = extract_answer_from_pbjson(decoded_pbjson, topN)
        elif videoType == 1:   # short answer
            extracted_webanswer_array: JsonList = extract_short_answer_from_pbjson(decoded_pbjson, topN)
        else:   # NotImplemeted
            return []
        # return [webanswer.get('Snippet', '') for webanswer in extracted_webanswer_array]
        return extracted_webanswer_array
    except Exception as e:
        logger.info(f"exception in extract_webanswer_parts, {e}")
        return []
    
def convert_length_to_unix_ts(lengthstring):
    # create a datetime object with the desired date and time
    try:
        items = lengthstring.split(':')
        sec = int(items[-1])
        hr, min = 0, 0
        if len(items) > 1:
            min = int(items[-2])
        if len(items) > 2:
            hr = int(items[-3])
        dt_start = datetime.datetime(2023, 11, 14, 0, 0, 0)
        dt_end = datetime.datetime(2023, 11, 14, hr, min, sec)

        # convert the datetime object to seconds since epoch
        ts_start = time.mktime(dt_start.timetuple())
        ts_end = time.mktime(dt_end.timetuple())
        # optionally, round the result to an integer or multiply it by 1000 to get milliseconds
        ts_int = int(ts_end) - int(ts_start)
    except:
        return -1
    return ts_int

def convert_time_to_unix_ts(timestring):
    try:
        # create a datetime object with the desired date and time, 4/1/2023 2:00:08 PM"
        items = timestring.split(' ')
        dates = items[0].split('/')
        month, day, year = int(dates[0]), int(dates[1]), int(dates[2])
        times = items[1].split(':')
        hr, min, sec = int(times[0]), int(times[1]), int(times[2])
        dt = datetime.datetime(year, month, day, hr, min, sec)

        # convert the datetime object to seconds since epoch
        ts = time.mktime(dt.timetuple())
        ts_int = int(ts)
    except:
        return -1
    return ts_int


def parse_bingAnswer_row(record):
    try:
        url = record[ "displayURL" ]
        if url.find('youtube') == -1 and url.find('tiktok') == -1:  # only consider the two source
            return None
        title = record[ "title" ]
        mediaSourceTitle = record.get('mediaSourceTitle', '') # YouTube/tiktok/...
        if mediaSourceTitle == '' and url.find('youtube') != -1:
            mediaSourceTitle = 'YouTube'
        elif mediaSourceTitle == '' and url.find('tiktok') != -1:
            mediaSourceTitle = 'TikTok'
        elif mediaSourceTitle == '':
            mediaSourceTitle = 'none'
        length = record.get('duration', -1)   # "15:00"
        if length != -1:
            length = convert_length_to_unix_ts(length)
        PubDate = record.get('publicationDate', -1)    # directly no need to convert
        viewcount = record.get('ViewCount', -1)
        PubUser = record.get('pubUser', 'none')
        channelPageLink = record.get('videoPageUrl', 'none')
        prismDocRow = f'{url}\t{channelPageLink}\t{PubUser}\t{title}\t{mediaSourceTitle}\t{PubDate}\t{length}\t{viewcount}'
        return prismDocRow
    except:
        return None


def parse_bingShortAnswer_row(record):
    try:
        url = record[ "Url" ]
        if url.find('youtube') == -1 and url.find('tiktok') == -1:  # only consider the two source
            return None
        title = record[ "Title" ]
        mediaSourceTitle = record.get('SourceTitle', '') # YouTube/tiktok/...
        if mediaSourceTitle == '' and url.find('youtube') != -1:
            mediaSourceTitle = 'YouTube'
        elif mediaSourceTitle == '' and url.find('tiktok') != -1:
            mediaSourceTitle = 'TikTok'
        elif mediaSourceTitle == '':
            mediaSourceTitle = 'none'
        length = record.get('TimeLength', -1)   # "15:00"
        PubDate = record.get('DAPublicationDate', -1)
        viewcount = record.get('ViewCount', -1)
        PubUser = record.get('DAPubUser', 'none')
        channelPageLink = record.get('videoPageUrl', 'none')
        prismDocRow = f'{url}\t{channelPageLink}\t{PubUser}\t{title}\t{mediaSourceTitle}\t{PubDate}\t{length}\t{viewcount}'
        return prismDocRow
    except:
        return None


def extract(input_path: str, videoType: int, topN: int) -> List[str]:
    """Extract specified columns from webanswers."""
    rets = ['query\tposition\tUrl\tChannelPageLink\tPubUser\tTitle\tMediaSourceTitle\tPubDate\tLength\tViewCount']
    fin = open(input_path, 'r', encoding='utf8')
    headers = fin.readline().split('\t')  # header
    query_ind = -1
    for i in range(len(headers)):
        if headers[i].lower() == 'query':
            query_ind = i
            break
    if query_ind == -1:
        logger.info(f"not found query column, ", headers)
    count = 0
    while True:
        count += 1
        line = fin.readline()
        if not line:
            break
        try:
            items = line.split('\t')
            decoded_pbjson = decode_base64_pbjson(items[0])
            query = items[query_ind].strip()
            results = decoded_pbjson['PropertyBag']['AnswerResponseCommand']['AnswerQueryResponse']['AnswerDataArray']
            has_answer = False
            for result in results:
                if videoType == 0:  # bing answer
                    if result.get('AnswerServiceName', None)  == 'MultimediaKifVideoAnswer':
                        videos = result['AnswerDataKifResponse'][0]['results']
                        for position in range(topN):
                            row = parse_bingAnswer_row(videos[position])
                            if row is not None:
                                ret = f'{query}\t{position}\t{row}'
                                rets.append(ret)
                                has_answer = True
                elif videoType == 1:    # bing short answer
                    if result.get('AnswerServiceName', None)  == 'MultimediaShortVideoAnswer':
                        videos = result['AnswerDataKifResponse'][0]['webResults']
                        for position in range(topN):
                            row = parse_bingShortAnswer_row(videos[position])
                            if row is not None:
                                ret = f'{query}\t{position}\t{row}'
                                rets.append(ret)
                                has_answer = True
                else:   # not implemented
                    continue
            if not has_answer:
                rets.append(f'{query}\t')
        except Exception as e:
            logger.info(f"exception in extract_webanswer_parts, {e}")
            continue

    return rets

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, type=str)
    parser.add_argument('--output', required=True, type=str)
    parser.add_argument('--videoType', required=True, type=int)
    parser.add_argument('--topN', required=True, type=int)
    parser.add_argument('--byline', required=False, type=int, default=1)
    args = parser.parse_args()

    logger.info(f"==> Loading input file {args.input}")
    if args.byline == 1:
        rets = extract(args.input, args.videoType, args.topN)
        row_count = len(rets)
        logger.info(f"==> exploding rows {row_count}")
        logger.info(f"==> Saving output file {args.output}")
        with open(args.output, 'w', encoding='utf8') as fout:
            fout.write('\n'.join(rets))
    else:
        logger.info(f"Not Implemented")
    # else:
    #     df = pd.read_csv(
    #         args.input,
    #         sep='\t',
    #         encoding='utf8',
    #         quoting=csv.QUOTE_NONE,
    #         usecols=['query','base64response']
    #         # base64response	isadultquery	isnoresults	language	position	query	region	scrapejobengineid
    #     )
    #     logger.info(f"==> Parsing base64 pbjson and extracting webanswers")
    #     df['webanswers'] = df['base64response'].apply(lambda x: extract_webanswer_parts(x, args.videoType, args.topN))
    #     logger.info(f"==> Exploding dataframe for each query-webanswer pair")
    #     df = df.explode('webanswers').dropna()
    #     df['url'] = pd.DataFrame(df['webanswers'], index=df.index)
    #     row_count = df.shape[0]
    #     logger.info(f"==> exploding rows {row_count}")
    #     logger.info(f"==> Saving output file {args.output}")
    #     df[['query', 'url']].to_csv(
    #         args.output,
    #         sep ='\t',
    #         encoding='utf8',
    #         quoting=csv.QUOTE_NONE,
    #         index=False
    #     )
