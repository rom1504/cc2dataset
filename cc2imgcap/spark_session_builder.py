"""spark session builder"""

from pyspark.sql import SparkSession
import os
import sys


def build_spark_session(master, num_cores, mem_gb):
    """Build a spark session based on the master url and the number of cores and memory to use"""
    spark = SparkSession.getActiveSession()

    if spark is None:
        print("No pyspark session found, creating one!")

        if master == "local":
            spark = local_session(num_cores, mem_gb)
        else:
            spark = aws_ec2_s3_spark_session(master, num_cores, mem_gb)

    return spark


def local_session(num_cores=4, mem_gb=16):
    """Build a local spark session"""
    spark = (
        SparkSession.builder.config("spark.driver.memory", str(mem_gb) + "G")
        .master("local[" + str(num_cores) + "]")
        .appName("cc2imgcap")
        .getOrCreate()
    )
    return spark


def aws_ec2_s3_spark_session(master, num_cores=128, mem_gb=256):
    """Build a spark session on AWS EC2"""
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    main_memory = str(int(mem_gb * 0.8)) + "g"
    memory_overhead = str(mem_gb - int(mem_gb * 0.8)) + "g"
    spark = (
        SparkSession.builder.config("spark.submit.deployMode", "client")
        .config("spark.executor.memory", main_memory)
        .config("spark.executor.cores", str(num_cores))  # this can be set to the number of cores of the machine
        .config("spark.task.cpus", "1")
        .config("spark.executor.memoryOverhead", memory_overhead)
        .config("spark.task.maxFailures", "2")
        .config(
            "spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1,org.apache.spark:spark-hadoop-cloud_2.13:3.3.1"
        )
        # change to the appropriate auth method, see https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.InstanceProfileCredentialsProvider")
        # ton of options to try and make s3a run faster
        .config("spark.hadoop.fs.s3a.threads.max", "512")
        .config("spark.hadoop.fs.s3a.connection.maximum", "2048")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
        .config("spark.sql.shuffle.partitions", "4000")
        .config("spark.hadoop.fs.s3a.directory.marker.retention", "keep")
        .config("spark.hadoop.fs.s3a.max.total.tasks", "512")
        .config("spark.hadoop.fs.s3a.multipart.threshold", "5M")
        .config("spark.hadoop.fs.s3a.multipart.size", "5M")
        .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "512")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config("spark.hadoop.fs.s3a.readahead.range", "2M")
        .config("spark.hadoop.fs.s3a.socket.recv.buffer", "65536")
        .config("spark.hadoop.fs.s3a.socket.send.buffer", "65536")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.hadoop.fs.s3a.experimental.input.fadvise", "random")
        .config("spark.hadoop.fs.s3a.block.size", "2M")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.fast.buffer.size", "100M")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "array")
        .config("spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled", "true")
        .master(master)  # this should be set to the spark master url
        .appName("cc2imgcap")
        .getOrCreate()
    )
    return spark
