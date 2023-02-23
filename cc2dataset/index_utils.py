import fsspec
from multiprocessing.pool import ThreadPool

def get_cc_links(source_cc_protocol,ccfile):
    """Get cc wat links"""
    if source_cc_protocol == "s3":
        fs, p = fsspec.core.url_to_fs("s3://commoncrawl/crawl-data/")
        if ccfile == "warc":
            links = ["s3://" + e for e in fs.glob(p + "/*/warc.paths.gz")]
        elif ccfile == "wat":
            links = ["s3://" + e for e in fs.glob(p + "/*/wat.paths.gz")]
        elif ccfile == "wet":
            links = ["s3://" + e for e in fs.glob(p + "/*/wet.paths.gz")]
        else:
            raise ValueError(f"Unknown ccfile: {ccfile}")
        return links
    elif source_cc_protocol == "http":
        fs, p = fsspec.core.url_to_fs("https://commoncrawl.org/the-data/get-started/")
        a = fs.open(p).read()
        l = a.splitlines()
        l = [e.decode("utf8").replace("[WARC] ", "") for e in l]
        l = [e for e in l if "<li>s3://commoncrawl/crawl-data/" in e]
        l = [
            e.split(" ")[0].replace("<li>s3://commoncrawl/", "https://data.commoncrawl.org/").replace("<wbr>", "")
            for e in l
        ]
        if ccfile == "warc":
            l = [(e + "/warc.paths.gz").replace("//warc", "/warc") for e in l]
        elif ccfile == "wat":
            l = [(e + "/wat.paths.gz").replace("//wat", "/wat") for e in l]
        elif ccfile == "wet":
            l = [(e + "/wet.paths.gz").replace("//wet", "/wet") for e in l]
        return l
    else:
        raise ValueError(f"Unknown protocol {source_cc_protocol}")


def read_index_file(file_index):
    with fsspec.open(file_index, "rb", compression="gzip") as f:
        crawlfiles = [a.decode("utf8").strip() for a in f.readlines()]
    return crawlfiles


def read_index_files(crawl_count, file_count, source_cc_protocol, ccfile, crawl_index_list, shuffle):
    """Read all wat index files"""
    cc_links = get_cc_links(source_cc_protocol,ccfile)
    if crawl_index_list is not None:
        cc_links = [cc_wat_links[i] for i in crawl_index_list]
    if crawl_count is not None:
        cc_links = cc_links[-crawl_count:]  # pylint: disable=invalid-unary-operand-type
    all_files = []
    with ThreadPool(16) as pool:
        for wats in pool.imap_unordered(read_index_file, cc_links):
            all_files.extend(wats)
    if file_count is not None:
        all_files = random.choices(all_files, k=file_count)
    if shuffle:
        # shuffle to increase duplication over each part hence reduce size of each part after duplication
        random.shuffle(all_files)
    return all_files