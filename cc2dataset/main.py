"""Easily convert common crawl to image caption set using pyspark"""


from fastwarc.warc import ArchiveIterator, WarcRecordType
from resiliparse.parse.html import HTMLTree
from resiliparse.extract.html2text import extract_plain_text
import simdjson
import fsspec
from timeit import default_timer as timer
from loguru import logger
import hashlib
import datetime
from multiprocessing.pool import ThreadPool
from pyspark import SparkContext
from pyspark.sql.functions import rand
from pyspark.sql import SparkSession
import random
import math
import time
from .spark_session_builder import build_spark_session
from io import BytesIO
from urllib.parse import urljoin
import fire


def valid_video_link(link):
    valid_video = any(
        link.get("url", "").endswith(ext) for ext in [".avi", ".mp4", ".mkv", ".webm", ".mov", ".mpg", ".mpeg", ".m4v"]
    )
    return valid_video


def extract_video_from_links(links):
    filtered_links = [{"url": link["url"], "alt": link.get("text", "")} for link in links if valid_video_link(link)]
    return filtered_links


text_extensions = set(
    [
        "pdf",
        "epub",
        "djvu",
        "mobi",
        "doc",
        "docx",
        "rtf",
        "txt",
        "odt",
        "ppt",
        "pptx",
        "pages",
        "keynote",
        "wps",
        "md",
    ]
)


def valid_text_link(link):
    splits = link.get("url", "").split(".")
    if len(splits) < 2:
        return False
    if splits[-1] not in text_extensions:
        return False
    return True


def extract_text_from_links(links):
    filtered_links = [{"url": link["url"], "alt": link.get("text", "")} for link in links if valid_text_link(link)]
    return filtered_links


def valid_audio_link(link):
    valid_audio = any(link.get("url", "").endswith(ext) for ext in [".ogg", ".wav", ".mp3", ".flac", ".m4a"])
    return valid_audio


def extract_audio_from_links(links):
    """Extract image from links"""
    filtered_links = [{"url": link["url"], "alt": link.get("text", "")} for link in links if valid_audio_link(link)]
    return filtered_links


def valid_image_link(link):
    valid_path = link.get("path", "") == "IMG@/src"
    valid_alt = len(link.get("alt", "")) > 0
    return valid_path and valid_alt


def extract_image_from_links(links):
    """Extract image from links"""
    filtered_links = [{"url": link["url"], "alt": link["alt"]} for link in links if valid_image_link(link)]
    return filtered_links


def valid_image_only_link(link):
    valid_path = link.get("path", "") == "IMG@/src"
    return valid_path


def extract_image_only_from_links(links):
    """Extract image from links even when no caption is present"""
    filtered_links = [{"url": link["url"], "alt": link.get("alt", "")} for link in links if valid_image_only_link(link)]
    return filtered_links


def make_link_absolute(url, base_url):
    if url.startswith("http://") or url.startswith("https://"):
        return url
    try:
        return urljoin(base_url, url)
    except ValueError:
        return url


def make_links_absolute(links, base_url):
    return [{"url": make_link_absolute(link["url"], base_url), "alt": link["alt"]} for link in links]


def extract_documents_from_links(links, document_type):
    """Extract documents from links ; this function returns a list of dict {"alt": ..., "url": ...}"""

    if document_type == "image":
        return extract_image_from_links(links)
    elif document_type == "image_only":
        return extract_image_only_from_links(links)
    elif document_type == "audio":
        return extract_audio_from_links(links)
    elif document_type == "text":
        return extract_text_from_links(links)
    elif document_type == "video":
        return extract_video_from_links(links)
    elif document_type == "video_platform":
        return extract_video_platform_from_links(links)
    else:
        raise ValueError(f"Unknown document type {document_type}")


def extract_documents_from_warc(path):
    """Extract documents from WARC"""
    with fsspec.open(path, mode="rb", compression="gzip") as f:
        try:
            for record in ArchiveIterator(f):
                try:
                    page_url = str(record.headers["WARC-Target-URI"])
                    tree = HTMLTree.parse_from_bytes(record.reader.read())

                    for ele in tree.body.get_elements_by_tag_name("iframe"):
                        alt = extract_plain_text(str(ele.parent))
                        url = urljoin(page_url, ele.getattr("src"))
                        if url not in [None, "anout:blank"]:

                            yield (str(hashlib.md5((alt + url).encode()).hexdigest()), url, alt, path, page_url)

                except:  # pylint: disable=bare-except
                    continue

        except Exception as e:  # pylint: disable=broad-except
            logger.info(e)
            logger.info("A shard failed to parse")


def extract_documents_from_wat(stream, document_type):
    """Extract document from stream"""
    all_links = []
    try:
        for record in ArchiveIterator(stream, record_types=WarcRecordType.metadata, parse_http=False):
            try:
                record_data = simdjson.load(record.reader)  # type: ignore
            except:  # pylint: disable=bare-except
                logger.info("A shard record failed")
                continue
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
            cc_filename = record_data["Container"]["Filename"]
            page_url = envelope["WARC-Header-Metadata"]["WARC-Target-URI"]
            # extract base URL to resolve relative URLs
            base_url = envelope["WARC-Header-Metadata"]["WARC-Target-URI"]
            if "Head" in metadata and "Base" in metadata["Head"]:
                try:
                    base_url = urljoin(base_url, metadata["Head"]["Base"])
                except ValueError:
                    pass

            filtered_links = extract_documents_from_links(links, document_type)
            filtered_links = make_links_absolute(filtered_links, base_url)
            filtered_links = [
                link
                for link in filtered_links
                if link["url"].startswith("http://") or link["url"].startswith("https://")
            ]
            for link in filtered_links:
                link["uid"] = str(hashlib.md5((link["alt"] + link["url"]).encode()).hexdigest())
                link["cc_filename"] = cc_filename
                link["page_url"] = page_url
            all_links.extend(filtered_links)
            # if len(all_links) > 100:
            #     return all_links
    except Exception as e:  # pylint: disable=broad-except
        logger.info(e)
        logger.info("A shard failed to parse")
        return []

    return all_links


def process_wat(path, document_type):
    """Process a single wat file"""

    ext = path.replace(".gz", "").split(".")[-1].replace("/", "").lower()
    if ext not in ["wat", "warc"]:
        raise ValueError(f"Extension can only be either 'wat' or 'warc', you provied {ext}")

    begin_read = timer()
    if ext == "warc" and document_type == "iframe":
        for e in extract_documents_from_warc(path):
            yield e
    else:
        with fsspec.open(path, mode="rb", compression="gzip") as f:
            for i in range(10):
                try:
                    tf = BytesIO(f.read())
                    break
                except Exception as ex:  # pylint: disable=broad-except
                    if i == 9:

                        logger.info("failed 10 times, skipping ", path)
                        return
                    logger.info(ex)
                    logger.info(f"retrying reading {i}/10")
                    time.sleep(1)

            if ext == "wat" and document_type != "iframe":
                for e in extract_documents_from_wat(tf, document_type):
                    yield (e["uid"], e["url"], e["alt"], e["cc_filename"], e["page_url"])
            elif ext == "wat" and document_type == "iframe":
                raise ValueError(f"Document type {document_type} is not suppeorted by file type {ext}")
            else:
                raise ValueError(f"Unknown document type {document_type} and file type {ext}")
    end_read = timer()
    tot_read_time = end_read - begin_read
    logger.info(f"Took {tot_read_time} to parse")


def get_cc_wat_links(source_cc_protocol, ext):
    """Get cc wat links"""
    if source_cc_protocol == "s3":
        fs, p = fsspec.core.url_to_fs("s3://commoncrawl/crawl-data/")
        links = ["s3://" + e for e in fs.glob(p + f"/*/{ext}.paths.gz")]
        return links
    elif source_cc_protocol == "http":
        fs, p = fsspec.core.url_to_fs("https://commoncrawl.org/the-data/get-started/")
        a = fs.open(p).read()
        l = a.splitlines()
        l = [e.decode("utf8").replace("[WARC] ", "") for e in l]
        l = [e for e in l if "<li>s3://commoncrawl/crawl-data/" in e]
        l = [
            e.split(" ")[0].replace("<li>s3://commoncrawl/", "https://data.commoncrawl.org/").replace("<wbr>", "")
            for e in l
        ]
        l = [(e + f"/{ext}.paths.gz").replace(f"//{ext}", f"/{ext}") for e in l]

        return l
    else:
        raise ValueError(f"Unknown protocol {source_cc_protocol}")


def read_wat_index_file(wat_index):
    with fsspec.open(wat_index, "rb", compression="gzip") as f:
        wats = [a.decode("utf8").strip() for a in f.readlines()]
    return wats


def read_wat_index_files(shard_count, wat_count, source_cc_protocol, ext):
    """Read all wat index files"""
    cc_wat_links = get_cc_wat_links(source_cc_protocol, ext)
    if shard_count is not None:
        cc_wat_links = cc_wat_links[-shard_count:]  # pylint: disable=invalid-unary-operand-type
    all_wats = []
    with ThreadPool(16) as pool:
        for wats in pool.imap_unordered(read_wat_index_file, cc_wat_links):
            all_wats.extend(wats)
    if wat_count is not None:
        all_wats = random.choices(all_wats, k=wat_count)
    else:
        # shuffle to increase duplication over each part hence reduce size of each part after duplication
        random.shuffle(all_wats)
    return all_wats


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


def process_one_part(output_path, wat_index_files, build_spark, shuffle, document_type, source_cc_protocol):
    """Process one part"""
    spark = build_spark()
    sc = SparkContext.getOrCreate()
    wat_count = len(wat_index_files)
    wat_rdd = sc.parallelize(wat_index_files, wat_count)
    if source_cc_protocol == "s3":
        prefix = "s3://commoncrawl/"
    elif source_cc_protocol == "http":
        prefix = "https://data.commoncrawl.org/"

    def extract(x):
        x = list(x)
        yield from process_wat(prefix + x[0], document_type)

    output = wat_rdd.mapPartitions(extract)
    df = output.toDF(["uid", "url", "alt", "cc_filename", "page_url"])
    df = df.na.drop(subset=["url"]).filter(df.url != "about:blank")

    deduplicate_repartition_count(df, output_path, wat_count, spark, shuffle)


def get_last_successful_part(output_path):
    """Get the last successful part"""
    output_path = output_path.replace("s3a", "s3")
    fs, _ = fsspec.core.url_to_fs(output_path)
    successful_parts = fs.glob(output_path + "/*/_SUCCESS")
    last_part = sorted([int(e.split("/")[-2].split("_")[-1]) for e in successful_parts if "merged" not in e])[-1]
    return last_part


def process_multi_part(
    output_path, wat_index_files, build_spark, multipart, shuffle, resume, document_type, source_cc_protocol
):
    """Process multi part"""
    if resume:
        start_part = get_last_successful_part(output_path) + 1
    else:
        start_part = 0

    wat_count = len(wat_index_files)
    wat_per_part = math.ceil(wat_count / multipart)
    part_paths = []
    for i in range(start_part, multipart):
        start = i * wat_per_part
        end = (i + 1) * wat_per_part
        part_path = f"{output_path}/part_{i}"
        part_paths.append(part_path)
        logger.info(f"Processing part {i} from {start} to {end} into {part_path}")
        process_one_part(part_path, wat_index_files[start:end], build_spark, False, document_type, source_cc_protocol)

    spark = build_spark()
    logger.info("Merging parts")
    df = None
    part_paths = [f"{output_path}/part_{i}" for i in range(0, multipart)]
    for part_path in part_paths:
        if df is None:
            df = spark.read.parquet(part_path)
        else:
            df = df.union(spark.read.parquet(part_path))

    deduplicate_repartition_count(df, output_path + "/merged", wat_count, spark, shuffle)


def get_date_str():
    return datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")


def cc2dataset(
    output_path,
    wat_index_count=1,
    wat_count=100,
    master="local",
    num_cores=128,
    mem_gb=256,
    multipart=None,
    shuffle=True,
    resume=None,
    spark_builder=None,
    document_type="image",
    source_cc_protocol="s3",
    file_type="wat",
):
    """Convert common crawl to image caption set"""

    file_type = file_type.lower()

    if file_type not in ["wat", "warc"]:
        raise ValueError("File type can only be either 'wat' or 'warc'")

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

        def spark_builder():
            return build_spark_session(master, num_cores, mem_gb)

    def build_spark():
        spark = SparkSession.getActiveSession()
        if spark is not None:
            spark.stop()
        return spark_builder()

    if resume is None:
        wat_index_files = read_wat_index_files(wat_index_count, wat_count, source_cc_protocol, file_type)
        # write wat index files to disk in output_path with fsspec
        with fsspec.open(f"{output_path}/{file_type}_index_files.txt", "w", encoding="utf8") as f:
            f.write("\n".join(wat_index_files))
    else:
        with fsspec.open(f"{output_path}/{file_type}_index_files.txt", "r", encoding="utf8") as f:
            wat_index_files = f.read().splitlines()

    if multipart is None:
        process_one_part(output_path, wat_index_files, build_spark, shuffle, document_type, source_cc_protocol)
    else:
        process_multi_part(
            output_path, wat_index_files, build_spark, multipart, shuffle, resume, document_type, source_cc_protocol
        )


def main():
    fire.Fire(cc2dataset)


if __name__ == "__main__":
    main()
