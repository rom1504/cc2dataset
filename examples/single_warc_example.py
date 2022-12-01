from cc2imgcap import process_wat
import os
import pandas as pd

# wget  https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wat/CC-MAIN-20220924151538-20220924181538-00000.warc.wat.gz -O sample_wat.tar.gz

if __name__ == "__main__":
    from_s3 = True
    if from_s3:
        results = process_wat(
            "s3://commoncrawl/crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wat/CC-MAIN-20220924151538-20220924181538-00000.warc.wat.gz",
        )
    else:
        results = process_wat("sample_wat.tar.gz")

    df = pd.DataFrame(results, columns=["uid", "url", "alt"])
    df.to_parquet(os.getcwd() + "/output.parquet")
