def extract_documents_from_warc(stream):
    """Extract document from stream"""
    all_extend = []
    try:
        for idx,record in enumerate(ArchiveIterator(f, max_content_length=4 * 1024**2)):
            if record.headers is None:
                continue
            if record.http_headers is None:
                continue
            if (
                record.headers["WARC-Type"] == "response"
                and record.content_length >= 128
            ):
                content_type = str(record.http_content_type).lower()
                if content_type.startswith("text/html"):
                    url = str(record.headers["WARC-Target-URI"])
                    html_bytes = record.reader.read()
                    encoding = detect_encoding(html_bytes)
                    tree = HTMLTree.parse_from_bytes(html_bytes, encoding)

                    for ele in tree.body.get_elements_by_tag_name("nav"):
                        ele.parent.remove_child(ele)


                    text = extract_plain_text(tree, preserve_formatting=False,
                                                main_content=False, list_bullets=False,
                                                alt_texts=True, links=False,
                                                form_fields=False, noscript=False)
                    text = text.replace("\n", " ").replace("\t", " ").replace("\r", " ")
                    all_extend.append({"text":text,"url":url,"uid":str(hashlib.md5((text+url).encode()).hexdigest())})
    except Exception as e:  # pylint: disable=broad-except
        logger.info(e)
        logger.info("A shard failed to parse")
        return []

    return all_extend

def process_warc(path):
    """Process a single warc file"""
    begin_read = timer()
    with fsspec.open(path, "rb") as f:
        for i in range(10):
            try:
                tf = BytesIO(f.read())
                break
            except Exception as ex:  # pylint: disable=broad-except
                if i == 9:
                    logger.info("failed 10 times, skipping ", path)
                    return
                logger.info(ex)
                logger.info(f"retrying reading {i}/10")
                time.sleep(1)

        for e in extract_documents_from_warc(tf):
            yield (e["uid"], e["url"], e["text"])
    end_read = timer()
    tot_read_time = end_read - begin_read
    logger.info(f"Took {tot_read_time} to parse")