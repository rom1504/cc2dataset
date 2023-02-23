from cc2dataset import process_warc
import os
import pandas as pd

if __name__ == "__main__":
    from_s3 = True
    wurl = "crawl-data/CC-MAIN-2022-33/segments/1659882570651.49/warc/CC-MAIN-20220807150925-20220807180925-00000.warc.gz"
    if from_s3:
        url = "s3://commoncrawl/" + wurl
    else:
        url = "https://data.commoncrawl.org/" + wurl

    #results = process_wat(url, "image")
    results = process_warc(url)
    df = pd.DataFrame(results, columns=["uid", "url", "text",'lang','license'])
    df.to_parquet(os.getcwd() + "/output.parquet")
    print(df)
