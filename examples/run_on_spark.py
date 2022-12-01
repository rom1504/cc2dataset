from cc2imgcap import cc2imgcap
import os

if __name__ == "__main__":
    # you can also create a spark session manually here and it will be picked up by cc2imgcap

    # if you have a slurm cluster, refer to https://gist.github.com/rom1504/67ada3dedbecc113ae2dbdfd9c642d83 to start a spark cluster there
    cc2imgcap(
        "s3a://s-laion/cc-proc-test/outputs",
        wat_index_count=None,
        wat_count=1000,
        master="spark://cpu128-dy-r6i-32xlarge-3:7077",
        num_cores=128,
        mem_gb=256,
        multipart=None,
    )
