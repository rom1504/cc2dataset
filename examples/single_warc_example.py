from cc2imgcap import extract_images_and_write_warc
import os

# wget  https://data.commoncrawl.org/crawl-data/CC-MAIN-2022-40/segments/1664030331677.90/wat/CC-MAIN-20220924151538-20220924181538-00000.warc.wat.gz -O sample_wat.tar.gz

if __name__ == "__main__":
    pth = ("sample_wat.tar.gz")
    extract_images_and_write_warc(0, [pth],  os.getcwd())