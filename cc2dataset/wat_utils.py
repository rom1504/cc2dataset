import hashlib
from fastwarc.warc import ArchiveIterator, WarcRecordType
import simdjson
import fsspec
from timeit import default_timer as timer
from loguru import logger
from io import BytesIO

def valid_video_link(link):
    valid_http = link.get("url", "").startswith("http")
    valid_video = any(
        link.get("url", "").endswith(ext) for ext in [".avi", ".mp4", ".mkv", ".webm", ".mov", ".mpg", ".mpeg", ".m4v"]
    )
    return valid_http and valid_video


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
    if not link.get("url", "").startswith("http"):
        return False
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
    valid_http = link.get("url", "").startswith("http")
    valid_audio = any(link.get("url", "").endswith(ext) for ext in [".ogg", ".wav", ".mp3", ".flac", ".m4a"])
    return valid_http and valid_audio


def extract_audio_from_links(links):
    """Extract image from links"""
    filtered_links = [{"url": link["url"], "alt": link.get("text", "")} for link in links if valid_audio_link(link)]
    return filtered_links


def valid_image_link(link):
    valid_path = link.get("path", "") == "IMG@/src"
    valid_alt = len(link.get("alt", "")) > 0
    valid_http = link.get("url", "").startswith("http")
    return valid_path and valid_http and valid_alt


def extract_image_from_links(links):
    """Extract image from links"""
    filtered_links = [{"url": link["url"], "alt": link["alt"]} for link in links if valid_image_link(link)]
    return filtered_links


def extract_documents_from_links(links, document_type):
    """Extract documents from links ; this function returns a list of dict {"alt": ..., "url": ...}"""

    if document_type == "image":
        return extract_image_from_links(links)
    elif document_type == "audio":
        return extract_audio_from_links(links)
    elif document_type == "text":
        return extract_text_from_links(links)
    elif document_type == "video":
        return extract_video_from_links(links)
    else:
        raise ValueError(f"Unknown document type {document_type}")


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

            filtered_links = extract_documents_from_links(links, document_type)
            for link in filtered_links:
                link["uid"] = str(hashlib.md5((link["alt"] + link["url"]).encode()).hexdigest())
            all_links.extend(filtered_links)
    except Exception as e:  # pylint: disable=broad-except
        logger.info(e)
        logger.info("A shard failed to parse")
        return []

    return all_links

def process_wat(path, document_type):
    """Process a single wat file"""
    begin_read = timer()
    with fsspec.open(path, "rb") as f:
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

        for e in extract_documents_from_wat(tf, document_type):
            yield (e["uid"], e["url"], e["alt"])
    end_read = timer()
    tot_read_time = end_read - begin_read
    logger.info(f"Took {tot_read_time} to parse")

