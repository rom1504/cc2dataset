"""Easily convert common crawl to image caption set using pyspark"""


from fastwarc.warc import ArchiveIterator, WarcRecordType
import simdjson
import fsspec
from timeit import default_timer as timer
from loguru import logger
import hashlib
import datetime

from pyspark import SparkContext
from pyspark.sql.functions import rand
from pyspark.sql import SparkSession
import random
import math
import time
from .spark_session_builder import build_spark_session
from .wat_utils import process_wat
from .warc_utils import process_warc
from .index_utils import read_index_files




def deduplicate_repartition_count(df, output_path, wat_count, spark, shuffle=False):
    """Deduplicate and repartition"""
    uniques = df.dropDuplicates(["uid"])
    s = time.time()
    if shuffle:
        uniques = uniques.sort(rand())
    repartitioned = uniques.repartition(max(256, wat_count // 500))
    repartitioned.write.mode("overwrite").parquet(output_path)
    e = time.time()
    logger.info(f"Took {e - s} seconds")
    logger.info("Computing size")
    df = spark.read.parquet(output_path)
    logger.info(f"Size: {df.count()}")


def process_one_part(output_path, cc_index_files, build_spark, shuffle, document_type, source_cc_protocol,ccfile):
    """Process one part"""
    spark = build_spark()
    sc = SparkContext.getOrCreate()
    ccfile_count = len(cc_index_files)
    wat_rdd = sc.parallelize(cc_index_files, ccfile_count)

    if source_cc_protocol == "s3":
        prefix = "s3://commoncrawl/"
    elif source_cc_protocol == "http":
        prefix = "https://data.commoncrawl.org/"

    if ccfile == "warc":
        def extract(x):
            x = list(x)
            yield from process_warc(prefix + x[0])

    elif ccfile == "wat":
        def extract(x):
            x = list(x)
            yield from process_wat(prefix + x[0], document_type)

    elif ccfile == "wet":
        def extract(x):
            x = list(x)
            yield from process_wet(prefix + x[0])
    else:
        raise ValueError(f"Unknown ccfile: {ccfile}")


    output = wat_rdd.mapPartitions(extract)
    # e["uid"], e["url"], e["text"],e['lang'],e['license'],e['perplexity']
    df = output.toDF(["uid", "url", "text","lang","license"])

    deduplicate_repartition_count(df, output_path, ccfile_count, spark, shuffle)


def get_last_successful_part(output_path):
    """Get the last successful part"""
    output_path = output_path.replace("s3a", "s3")
    fs, _ = fsspec.core.url_to_fs(output_path)
    successful_parts = fs.glob(output_path + "/*/_SUCCESS")
    last_part = sorted([int(e.split("/")[-2].split("_")[-1]) for e in successful_parts if "merged" not in e])[-1]
    return last_part


def process_multi_part(
    output_path, cc_index_files, build_spark, multipart, shuffle, resume, document_type, source_cc_protocol,ccfile
):
    """Process multi part"""
    if resume:
        start_part = get_last_successful_part(output_path) + 1
    else:
        start_part = 0

    ccfile_count = len(cc_index_files)
    ccfile_per_part = math.ceil(ccfile_count / multipart)
    part_paths = []
    for i in range(start_part, multipart):
        start = i * ccfile_per_part
        end = (i + 1) * ccfile_per_part
        part_path = f"{output_path}/part_{i}"
        part_paths.append(part_path)
        logger.info(f"Processing part {i} from {start} to {end} into {part_path}")
        process_one_part(part_path, cc_index_files[start:end], build_spark, False, document_type, source_cc_protocol,ccfile)

    spark = build_spark()
    logger.info("Merging parts")
    df = None
    part_paths = [f"{output_path}/part_{i}" for i in range(0, multipart)]
    for part_path in part_paths:
        if df is None:
            df = spark.read.parquet(part_path)
        else:
            df = df.union(spark.read.parquet(part_path))

    deduplicate_repartition_count(df, output_path + "/merged", ccfile_count, spark, shuffle)


def get_date_str():
    return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


def cc2dataset(
    output_path,
    crawl_index_count=1,
    files_count=100,
    master="local",
    num_cores=128,
    mem_gb=256,
    multipart=None,
    shuffle=True,
    resume=None,
    spark_builder=None,
    document_type="image",
    source_cc_protocol="s3",
    ccfile="wat",
    crawl_index_list=None,
):
    """Convert common crawl to image caption set"""

    if resume is not None and multipart is None:
        raise ValueError("Cannot resume without multipart")

    if resume is None:
        job_id = get_date_str()
        logger.info(f"JOB ID: {job_id}")
        output_path = f"{output_path}/{job_id}"
    else:
        output_path = resume

    logger.info(f"Writing in: {output_path}")

    if spark_builder is None:
        spark_builder = lambda: build_spark_session(master, num_cores, mem_gb)

    def build_spark():
        spark = SparkSession.getActiveSession()
        if spark is not None:
            spark.stop()
        return spark_builder()

    if resume is None:
        cc_index_files = read_index_files(crawl_index_count, files_count, source_cc_protocol, ccfile, crawl_index_list, shuffle )
        # write ccfile index files to disk in output_path with fsspec
        with fsspec.open(f"{output_path}/crawl_index_files.txt", "w", encoding="utf8") as f:
            f.write("\n".join(cc_index_files))
    else:
        with fsspec.open(f"{output_path}/crawl_index_files.txt", "r", encoding="utf8") as f:
            cc_index_files = f.read().splitlines()

    if multipart is None:
        process_one_part(output_path, cc_index_files, build_spark, shuffle, document_type, source_cc_protocol,ccfile)
    else:
        process_multi_part(
            output_path, cc_index_files, build_spark, multipart, shuffle, resume, document_type, source_cc_protocol, ccfile)


def main():
    fire.Fire(cc2dataset)


if __name__ == "__main__":
    main()
