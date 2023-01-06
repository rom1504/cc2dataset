from cc2dataset import process_wat
import os
import pandas as pd

if __name__ == "__main__":
    from_s3 = False
    wat = "crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wat/CC-MAIN-20220924151538-20220924181538-00000.warc.wat.gz"
    if from_s3:
        url = "s3://commoncrawl/" + wat
    else:
        url = "https://data.commoncrawl.org/" + wat

    results = process_wat(url, "video_platform")
    df = pd.DataFrame(results, columns=["uid", "url", "alt", "cc_filename", "page_url"])
    df.to_parquet(os.getcwd() + "/output.parquet")
    print(df)
