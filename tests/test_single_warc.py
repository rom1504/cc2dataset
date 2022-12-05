import pytest
from cc2dataset import process_wat
import pandas as pd

test_url = "https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wat/CC-MAIN-20220924151538-20220924181538-00000.warc.wat.gz"


def test_single_warc_image():
    results = process_wat(test_url, "image")
    df = pd.DataFrame(results, columns=["uid", "url", "alt"])
    assert len(df) == 240033
    assert df["uid"][3] == "ee8ab8628552d88a099129cf1a452745"


def test_single_warc_audio():
    results = process_wat(test_url, "audio")
    df = pd.DataFrame(results, columns=["uid", "url", "alt"])
    assert len(df) == 721
    assert df["uid"][3] == "fd38d5c43140dfda889566eddd8755c0"


def test_single_warc_text():
    results = process_wat(test_url, "text")
    df = pd.DataFrame(results, columns=["uid", "url", "alt"])
    assert len(df) == 5920
    assert df["uid"][3] == "93bae45778122b4ae8f79d420eaed667"


def test_single_warc_video():
    results = process_wat(test_url, "video")
    df = pd.DataFrame(results, columns=["uid", "url", "alt"])
    assert len(df) == 501
    assert df["uid"][3] == "a8f5837e354808f319d2a4899089090c"
