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

cols = ["uid", "url", "alt", "cc_filename", "page_url"]

def test_single_warc_image():
    results = retry(lambda: process_wat(test_url, "image"))
    df = pd.DataFrame(results, columns=cols)
    assert len(df) == 405232
    assert df["uid"][3] == "2a64f921d7ced2fed91e82eeb56338cd"


def test_single_warc_audio():
    results = retry(lambda: process_wat(test_url, "audio"))
    df = pd.DataFrame(results, columns=cols)
    assert len(df) == 927
    assert df["uid"][3] == "5c835ccd44d718e0a95d74b4a2902dfe"


def test_single_warc_text():
    results = retry(lambda: process_wat(test_url, "text"))
    df = pd.DataFrame(results, columns=cols)
    assert len(df) == 10552
    assert df["uid"][3] == "b485d42a0fad04a4e7e2fdb114e341c8"


def test_single_warc_video():
    results = retry(lambda: process_wat(test_url, "video"))
    df = pd.DataFrame(results, columns=cols)
    assert len(df) == 676
    assert df["uid"][3] == "a8f5837e354808f319d2a4899089090c"
