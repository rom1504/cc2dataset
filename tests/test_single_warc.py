import pytest
from cc2dataset import process_wat
import pandas as pd

test_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wat/CC-MAIN-20220924151538-20220924181538-00000.warc.wat.gz"


def retry(f, n=3):
    try:
        return f()
    except Exception as e:
        if n == 0:
            raise e
        else:
            print(e)
            return retry(f, n - 1)


def test_single_warc_image():
    results = retry(lambda: process_wat(test_url, "image"))
    df = pd.DataFrame(results, columns=["uid", "url", "alt"])
    assert len(df) == 240033
    assert df["uid"][3] == "ee8ab8628552d88a099129cf1a452745"


def test_single_warc_audio():
    results = retry(lambda: process_wat(test_url, "audio"))
    df = pd.DataFrame(results, columns=["uid", "url", "alt"])
    assert len(df) == 721
    assert df["uid"][3] == "fd38d5c43140dfda889566eddd8755c0"


def test_single_warc_text():
    results = retry(lambda: process_wat(test_url, "text"))
    df = pd.DataFrame(results, columns=["uid", "url", "alt"])
    assert len(df) == 6375
    assert df["uid"][3] == "b485d42a0fad04a4e7e2fdb114e341c8"


def test_single_warc_video():
    results = retry(lambda: process_wat(test_url, "video"))
    df = pd.DataFrame(results, columns=["uid", "url", "alt"])
    assert len(df) == 508
    assert df["uid"][3] == "a8f5837e354808f319d2a4899089090c"
