"""Easily convert common crawl to image caption set using pyspark"""


from fastwarc.warc import ArchiveIterator, WarcRecordType
from typing import BinaryIO
import simdjson
import fsspec
import pandas as pd
from timeit import default_timer as timer
from loguru import logger
import hashlib
from multiprocessing.pool import ThreadPool
from pyspark.sql import SparkSession
from pyspark import SparkContext
import random
import uuid
import time


def extract_imgs(stream: BinaryIO):
    """Extract images from a wat file"""
    all_links = []
    total = 0
    for record in ArchiveIterator(stream, record_types=WarcRecordType.metadata, parse_http=False):
        try:
            record_data = simdjson.load(record.reader)  # type: ignore
        except:  # pylint: disable=bare-except
            continue
        # print(record_data)
        envelope = record_data["Envelope"]
        payload = envelope["Payload-Metadata"]
        if "HTTP-Response-Metadata" not in payload:
            continue
        http_resp = payload["HTTP-Response-Metadata"]
        if "HTML-Metadata" not in http_resp:
            continue
        metadata = http_resp["HTML-Metadata"]
        if "Links" not in metadata:
            continue

        links = metadata["Links"]
        total += len(links)

        filtered_links = [{"url": link["url"], "alt": link["alt"]} for link in links if valid_link(link)]
        for link in filtered_links:
            link["uid"] = str(hashlib.md5((link["alt"] + link["url"]).encode()).hexdigest())
        all_links.extend(filtered_links)

    return all_links


def valid_link(link):
    valid_path = link.get("path", "") == "IMG@/src"
    valid_img = link.get("url", "").endswith((".png", ".jpg", ".jpeg"))
    valid_alt = len(link.get("alt", "")) > 0
    valid_http = link.get("url", "").startswith("http")
    return (valid_path or valid_img) and valid_path and valid_http and valid_alt


def url_is_img(url):
    rsp = url.lower().endswith((".png", ".jpg", ".jpeg"))
    valid_http = rsp.startswith("http")
    return rsp and valid_http


def process_wat(path, output_path):
    """Process a single wat file"""
    ret = {}
    s = timer()
    with fsspec.open(path, "rb") as f:
        all_img_records = extract_imgs(f)
    e = timer()
    tot_read_time = e - s
    ret["read_time"] = tot_read_time
    s = timer()
    df = pd.DataFrame.from_records(all_img_records)

    logger.info(f"Took {tot_read_time} to parse")
    with fsspec.open(f"{output_path}.parquet", "wb") as f2:
        df.to_parquet(f2)
    e = timer()
    tot_write_time = e - s
    ret["write_time"] = tot_write_time
    logger.info(f"Took {tot_write_time} to write to S3")
    return ret


def build_spark_session():
    spark = SparkSession.getActiveSession()

    if spark is None:
        print("No pyspark session found, creating a new one!")
        spark = (
            SparkSession.builder.config("spark.driver.memory", "16G")
            .master("local[4]")
            .appName("spark-stats")
            .getOrCreate()
        )


def get_cc_wat_links():
    fs, p = fsspec.core.url_to_fs("s3://commoncrawl/crawl-data/")
    links = ["s3://" + e for e in fs.glob(p + "/*/wat.paths.gz")]
    return links


def read_wat_index_file(wat_index):
    with fsspec.open(wat_index, "rb", compression="gzip") as f:
        wats = [a.decode("utf8").strip() for a in f.readlines()]
    return wats


def read_wat_index_files(shard_count=None, wat_count=None):
    """Read all wat index files"""
    cc_wat_links = get_cc_wat_links()
    if shard_count is not None:
        cc_wat_links = cc_wat_links[-shard_count:]  # pylint: disable=invalid-unary-operand-type
    all_wats = []
    with ThreadPool(16) as pool:
        for wats in pool.imap_unordered(read_wat_index_file, cc_wat_links):
            all_wats.extend(wats)
    if wat_count is not None:
        all_wats = random.choices(all_wats, k=wat_count)
    return all_wats


def cc2imgcap(output_path, wat_index_count=1, wat_count=100):
    """Convert common crawl to image caption set"""
    build_spark_session()

    sc = SparkContext.getOrCreate()
    wat_index_files = read_wat_index_files(wat_index_count, wat_count)
    wat_count = len(wat_index_files)
    wat_rdd = sc.parallelize(wat_index_files, wat_count)
    job_id = uuid.uuid4()
    logger.info(f"JOB ID: {job_id}")

    def extract(i, x):
        x = list(x)
        process_wat("s3://commoncrawl/" + x[0], output_path=f"{output_path}/{job_id}/{i}")
        return [0]

    output = wat_rdd.mapPartitionsWithIndex(extract)
    s = time.time()
    output.collect()
    e = time.time()
    print("Took ", e - s, "Seconds")


def main():
    fire.Fire(cc2imgcap)


if __name__ == "__main__":
    main()
