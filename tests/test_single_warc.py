import pytest
from cc2imgcap import process_wat
import pandas as pd


def test_single_warc():
    from_s3 = False
    wat = "crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wat/CC-MAIN-20220924151538-20220924181538-00000.warc.wat.gz"
    if from_s3:
        url = "s3://commoncrawl/" + wat
    else:
        url = "https://data.commoncrawl.org/" + wat

    results = process_wat(url, "image")
    df = pd.DataFrame(results, columns=["uid", "url", "alt"])
    assert len(df) == 240033
    assert df["uid"][3] == "ee8ab8628552d88a099129cf1a452745"
