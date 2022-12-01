"""Easily convert common crawl to image caption set using pyspark"""


from fastwarc.warc import ArchiveIterator, WarcRecordType
from typing import BinaryIO
import simdjson
import fsspec
from timeit import default_timer as timer
from loguru import logger
import hashlib
from multiprocessing.pool import ThreadPool
from pyspark import SparkContext
import random
import uuid
import time
from .spark_session_builder import build_spark_session


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


def process_wat(path):
    """Process a single wat file"""
    ret = {}
    s = timer()
    with fsspec.open(path, "rb") as f:
        for e in extract_imgs(f):
            yield (e["uid"], e["url"], e["alt"])
    e = timer()
    tot_read_time = e - s
    ret["read_time"] = tot_read_time
    s = timer()
    logger.info(f"Took {tot_read_time} to parse")


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


def cc2imgcap(output_path, wat_index_count=1, wat_count=100, master="local", num_cores=128, mem_gb=256):
    """Convert common crawl to image caption set"""
    spark = build_spark_session(master, num_cores, mem_gb)

    sc = SparkContext.getOrCreate()
    wat_index_files = read_wat_index_files(wat_index_count, wat_count)
    wat_count = len(wat_index_files)
    wat_rdd = sc.parallelize(wat_index_files, wat_count)
    job_id = uuid.uuid4()
    logger.info(f"JOB ID: {job_id}")
    full_output_path = f"{output_path}/{job_id}"
    logger.info(f"Writing in: {full_output_path}")

    def extract(x):
        x = list(x)
        yield from process_wat("s3://commoncrawl/" + x[0])

    output = wat_rdd.mapPartitions(extract)
    s = time.time()
    df = output.toDF(["uid", "url", "alt"])

    uniques = df.dropDuplicates(["uid"])
    repartitioned = uniques.repartition(max(256, wat_count // 100))
    repartitioned.write.parquet(full_output_path)
    e = time.time()
    print("Took ", e - s, "Seconds")
    print("Computing size")
    df = spark.read.parquet(full_output_path)
    print("Size: ", df.count())


def main():
    fire.Fire(cc2imgcap)


if __name__ == "__main__":
    main()
