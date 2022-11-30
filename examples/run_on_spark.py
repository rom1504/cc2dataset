# https://gist.github.com/rom1504/67ada3dedbecc113ae2dbdfd9c642d83

from cc2imgcap import cc2imgcap
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


if __name__ == "__main__":

    # this is an example using a virtualenv with cc2imgcap installed
    # you may also use the pex file with settings like this:
    # os.environ['PYSPARK_PYTHON'] = "full/path/cc2imgcap.pex"
    # .config("spark.executorEnv.PEX_ROOT", "./.pex")

    # edit to the path of your venv
    os.environ["PYSPARK_PYTHON"] = "/fsx/home-rom1504/cc2imgcap/.env/bin/python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "/fsx/home-rom1504/cc2imgcap/.env/bin/python"

    spark = (
        SparkSession.builder.config("spark.submit.deployMode", "client")
        .config("spark.executor.memory", "80GB")
        .config("spark.executor.cores", "128")  # this can be set to the number of cores of the machine
        .config("spark.task.cpus", "1")
        .config("spark.executor.memoryOverhead", "16GB")
        .config("spark.task.maxFailures", "2")
        .master("spark://cpu128-dy-c6i-32xlarge-11:7077")  # this should be set to the spark master url
        .appName("spark-stats")
        .getOrCreate()
    )

    cc2imgcap("s3://s-laion/cc-proc-test/outputs", wat_index_count=None, wat_count=500000, wat_per_output_file=100)
