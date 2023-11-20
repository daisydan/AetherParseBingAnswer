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
        decoded_pbjson: JsonDict) -> JsonList:
    """Extract webanswers from pbjson."""
    answer_data_array: JsonList = decoded_pbjson.get('PropertyBag', None).get('AnswerResponseCommand', None).get('AnswerQueryResponse', None).get('AnswerDataArray', None)
    for element in answer_data_array:
        if element.get('AnswerServiceName', None) == 'MultimediaKifVideoAnswer':
            answer_data_kif_response: JsonList = element.get('AnswerDataKifResponse', None)
            web_answers: JsonList = answer_data_kif_response[0].get('results', None)
            return web_answers
    return [{}]


def extract_short_answer_from_pbjson(
        decoded_pbjson: JsonDict) -> JsonList:
    """Extract webanswers from pbjson."""
    answer_data_array: JsonList = decoded_pbjson.get('PropertyBag', None).get('AnswerResponseCommand', None).get('AnswerQueryResponse', None).get('AnswerDataArray', None)
    for element in answer_data_array:
        if element.get('AnswerServiceName', None) == 'MultimediaShortVideoAnswer':
            answer_data_kif_response: JsonList = element.get('AnswerDataKifResponse', None)
            web_answers: JsonList = answer_data_kif_response[0].get('webResults', None)
            return web_answers
    return [{}]


def extract_webanswer_parts(
        encoded_base64_pbjson: str, videoType: int) -> List[str]:
    """Extract specified columns from webanswers."""
    try:
        decoded_pbjson: JsonDict = decode_base64_pbjson(encoded_base64_pbjson)
        if videoType == 0:    # bing answer
            extracted_webanswer_array: JsonList = extract_answer_from_pbjson(decoded_pbjson)
        elif videoType == 1:   # short answer
            extracted_webanswer_array: JsonList = extract_short_answer_from_pbjson(decoded_pbjson)
        else:   # NotImplemeted
            return []
        # return [webanswer.get('Snippet', '') for webanswer in extracted_webanswer_array]
        return extracted_webanswer_array
    except Exception as e:
        return []


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--input', required=True, type=str)
    parser.add_argument('--output', required=True, type=str)
    parser.add_argument('--videoType', required=True, type=int)
    args = parser.parse_args()

    logger.info(f"==> Loading input file {args.input}")
    df = pd.read_csv(
        args.input,
        sep='\t',
        encoding='utf8',
        quoting=csv.QUOTE_NONE,
        usecols=['query','base64response']
        # base64response	isadultquery	isnoresults	language	position	query	region	scrapejobengineid
    )
    logger.info(f"==> Parsing base64 pbjson and extracting webanswers")
    df['webanswers'] = df['base64response'].apply(lambda x: extract_webanswer_parts(x, args.videoType))
    logger.info(f"==> Exploding dataframe for each query-webanswer pair")
    df = df.explode('webanswers').dropna()
    df['url'] = pd.DataFrame(df['webanswers'].tolist(), index=df.index)
    logger.info(f"==> Saving output file {args.output}")
    df[['query', 'url']].to_csv(
        args.output,
        sep ='\t',
        encoding='utf8',
        quoting=csv.QUOTE_NONE,
        index=False
    )
