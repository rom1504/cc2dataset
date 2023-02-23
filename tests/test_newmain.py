import pytest
from cc2dataset import cc2dataset
from cc2dataset.index_utils import get_cc_links
import os
import pandas as pd
import tempfile
from glob import glob


def test_main():
    with tempfile.TemporaryDirectory() as tmpdir:
        cc2dataset(
            tmpdir,
            crawl_index_count=None,
            files_count=1,
            master="local",
            num_cores=1,
            mem_gb=2,
            multipart=None,
            source_cc_protocol="s3",
            shuffle=False,ccfile='warc',crawl_index_list=[-3]
        )
        files = list(glob(os.path.join(tmpdir, "*/*.parquet")))
        assert len(files) == 256
        df = pd.read_parquet(files)
        assert len(df) > 100
