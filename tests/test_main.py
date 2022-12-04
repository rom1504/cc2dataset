import pytest
from cc2imgcap import cc2imgcap
import os
import pandas as pd
import tempfile
from glob import glob


def test_main():
    with tempfile.TemporaryDirectory() as tmpdir:
        cc2imgcap(
            tmpdir,
            wat_index_count=None,
            wat_count=1,
            master="local",
            num_cores=1,
            mem_gb=2,
            multipart=None,
            source_cc_protocol="http",
        )
        files = list(glob(os.path.join(tmpdir, "*/*.parquet")))
        assert len(files) == 256
        df = pd.read_parquet(files[0])
        assert len(df) > 100
