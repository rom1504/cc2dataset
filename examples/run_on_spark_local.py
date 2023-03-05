from cc2dataset import cc2dataset
import os

if __name__ == "__main__":
    cc2dataset(
        "/media/hd/tmp_output_cc2dataset",
        wat_index_count=None,
        wat_count=100,
        master="local",
        num_cores=16,
        mem_gb=16,
        source_cc_protocol="http",
    )
