from cc2imgcap import process_wat
import os

# wget  https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wat/CC-MAIN-20220924151538-20220924181538-00000.warc.wat.gz -O sample_wat.tar.gz

if __name__ == "__main__":
    from_s3 = True
    if from_s3:
        process_wat(
            "s3://commoncrawl/crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wat/CC-MAIN-20220924151538-20220924181538-00000.warc.wat.gz",
            os.getcwd() + "/out.parquet",
        )
    else:
        process_wat("sample_wat.tar.gz", os.getcwd() + "/output.parquet")
