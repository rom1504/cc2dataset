"""Easily convert common crawl to image caption set using pyspark"""


from curses import meta
from email.mime import base
from re import I
from fastwarc.warc import ArchiveIterator, WarcRecordType
from fastwarc.stream_io import GZipStream
from typing import BinaryIO
import simdjson
import json
import pathlib
from tqdm import tqdm
import fsspec
import pandas  as pd
from timeit import default_timer as timer
from loguru import logger
import hashlib

def extract_imgs(stream: BinaryIO):
    all_links = []
    total = 0
    filtered = 0
    for record in ArchiveIterator(stream, record_types=WarcRecordType.metadata, parse_http=False):
        try:
            record_data = simdjson.load(record.reader)
        except:
            continue
        # print(record_data)
        envelope = record_data['Envelope']
        payload = envelope["Payload-Metadata"]
        if "HTTP-Response-Metadata" not in payload:
            continue
        http_resp = payload['HTTP-Response-Metadata']
        if "HTML-Metadata" not in http_resp:
            continue
        metadata = http_resp["HTML-Metadata"]
        if "Links" not in metadata:
            continue

        links = metadata['Links']
        total += len(links)

        filtered_links = [link for link in links if valid_link(link)]
        for link in filtered_links:
            link["uid"] = str(hashlib.md5((link["alt"] + link["url"]).encode()).hexdigest())
        all_links.extend(filtered_links)

    return all_links

def valid_link(link):
    valid_path = link.get("path", "") == 'IMG@/src'
    valid_img = link.get("url", "").endswith(('.png', '.jpg', '.jpeg'))
    valid_alt = len(link.get('alt', "")) > 0
    valid_http =  link.get("url", "").startswith("http") 
    return (valid_path or valid_img) and valid_path and valid_http and valid_alt

def url_is_img(url):
    rsp = url.lower().endswith(('.png', '.jpg', '.jpeg')) 
    valid_http = rsp.startswith("http")
    return rsp and valid_http


def read_s3_extract_images_and_write_warc(idx, paths, output_path):
    all_img_records = []
    ret = {}
    s = timer()
    for path in paths: 
        with fsspec.open(f"s3://commoncrawl/{path}", "rb") as f:
            all_img_records += extract_imgs(f)
    e = timer()
    tot_read_time = e - s
    ret["read_time"] = tot_read_time
    s = timer()
    df = pd.DataFrame.from_records(all_img_records)

    logger.info(f"Took {tot_read_time} to write to S3")
    with fsspec.open(f"{output_path}/{idx}.parquet", "wb") as f2:
        df.to_parquet(f2)
    e = timer()
    tot_write_time = e - s
    ret["write_time"] = tot_write_time
    logger.info(f"Took {tot_write_time} to write to S3")
    return ret

    
def extract_images_and_write_warc(idx, paths, output_path):
    s = timer()
    all_img_records = []
    for path in paths:
        with fsspec.open(path, "rb") as f:
            all_img_records += extract_imgs(f)
    e = timer()
    tot_time = e - s
    df = pd.DataFrame.from_records(all_img_records)
    with fsspec.open(f"{output_path}/{idx}.parquet", "wb") as f2:
        df.to_parquet(f2)