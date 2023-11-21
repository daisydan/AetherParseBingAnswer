"""Script to extract web answer details from scraped pbjson."""

import argparse
import base64
import csv
import json
import logging
from typing import Any, Dict, List, Tuple

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
    

def extract(input_path: str, videoType: int, topN: int) -> List[str]:
    """Extract specified columns from webanswers."""
    rets = ['query\turl']
    fin = open(input_path, 'r', encoding='utf8')
    fin.readline()  # header
    count = 0
    while True:
        count += 1
        # Get next line from file
        line = fin.readline()
        if not line:
            break
        # import pdb; pdb.set_trace()
        try:
            items = line.split('\t')
            decoded_pbjson = decode_base64_pbjson(items[0])
            query = items[1]
            results = decoded_pbjson['PropertyBag']['AnswerResponseCommand']['AnswerQueryResponse']['AnswerDataArray']
            has_answer = False
            for result in results:
                ret = ''
                if videoType == 0:  # bing answer
                    if result.get('AnswerServiceName', None)  == 'MultimediaKifVideoAnswer':
                        videos = result['AnswerDataKifResponse'][0]['results']
                        topk = ''
                        if len(videos) > 0:
                            topk = json.dumps(videos[:topN])
                        ret = f'{query}\t{topk}'
                elif videoType == 1:    # bing short answer
                    if result.get('AnswerServiceName', None)  == 'MultimediaShortVideoAnswer':
                        videos = result['AnswerDataKifResponse'][0]['webResults']
                        topk = ''
                        if len(videos) > 0:
                            topk = json.dumps(videos[:topN])
                        ret = f'{query}\t{topk}'
                else:
                    continue
                if ret != '':
                    rets.append(ret)
                    has_answer = True
                    break
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
    parser.add_argument('--byline', required=True, type=int)
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
        df = pd.read_csv(
            args.input,
            sep='\t',
            encoding='utf8',
            quoting=csv.QUOTE_NONE,
            usecols=['query','base64response']
            # base64response	isadultquery	isnoresults	language	position	query	region	scrapejobengineid
        )
        logger.info(f"==> Parsing base64 pbjson and extracting webanswers")
        df['webanswers'] = df['base64response'].apply(lambda x: extract_webanswer_parts(x, args.videoType, args.topN))
        logger.info(f"==> Exploding dataframe for each query-webanswer pair")
        df = df.explode('webanswers').dropna()
        df['url'] = pd.DataFrame(df['webanswers'], index=df.index)
        row_count = df.shape[0]
        logger.info(f"==> exploding rows {row_count}")
        logger.info(f"==> Saving output file {args.output}")
        df[['query', 'url']].to_csv(
            args.output,
            sep ='\t',
            encoding='utf8',
            quoting=csv.QUOTE_NONE,
            index=False
        )
