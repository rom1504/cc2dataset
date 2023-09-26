from cc2dataset import process_wat
import os
import pandas as pd

if __name__ == "__main__":
    from_s3 = False
    wat = (
        "crawl-data/CC-MAIN-2023-06/segments/1674764494974.98/warc/CC-MAIN-20230127065356-20230127095356-00752.warc.gz"
    )
    if from_s3:
        url = "s3://commoncrawl/" + wat
    else:
        url = "https://data.commoncrawl.org/" + wat

    results = process_wat(url, "iframe")
    df = pd.DataFrame(results, columns=["uid", "url", "alt", "cc_filename", "page_url"])
    df.to_parquet(os.getcwd() + "/output.parquet")
    print(df)
