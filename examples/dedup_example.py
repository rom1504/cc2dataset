# Example to deduplicate the result

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


if __name__ == "__main__":
    spark = (
        SparkSession.builder.config("spark.submit.deployMode", "client")
        .config("spark.executor.memory", "164GB")
        .config("spark.executor.cores", "128")  # this can be set to the number of cores of the machine
        .config("spark.task.cpus", "1")
        .config("spark.executor.memoryOverhead", "32GB")
        .config("spark.task.maxFailures", "2")
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1,org.apache.spark:spark-hadoop-cloud_2.13:3.3.1')
         # change to the appropriate auth method, see https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.InstanceProfileCredentialsProvider')
        # ton of options to try and make s3a run faster
        .config('spark.hadoop.fs.s3a.threads.max', '512')
        .config('spark.hadoop.fs.s3a.connection.maximum', '2048')
        .config('spark.hadoop.fs.s3a.fast.upload', 'true')
        .config('spark.sql.shuffle.partitions', '4000')
        .config('spark.hadoop.fs.s3a.directory.marker.retention', 'keep')
        .config('spark.hadoop.fs.s3a.max.total.tasks', '512')
        .config("spark.hadoop.fs.s3a.multipart.threshold", "5M")
        .config("spark.hadoop.fs.s3a.multipart.size", "5M")
        .config("spark.hadoop.fs.s3a.fast.upload.active.blocks", "512")
        .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
        .config("spark.hadoop.fs.s3a.connection.timeout", "600000")
        .config('spark.hadoop.fs.s3a.readahead.range', '2M')
        .config('spark.hadoop.fs.s3a.socket.recv.buffer', '65536')
        .config('spark.hadoop.fs.s3a.socket.send.buffer', '65536')
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config('spark.hadoop.fs.s3a.experimental.input.fadvise', 'random')
        .config('spark.hadoop.fs.s3a.block.size', '2M')
        .config('spark.hadoop.fs.s3a.connection.ssl.enabled', 'false')
        .config('spark.hadoop.fs.s3a.fast.buffer.size', '100M')
        .config('spark.hadoop.fs.s3a.fast.upload.buffer', 'array')
        .config('spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled', 'true')
        .master("spark://cpu128-dy-c6i-32xlarge-21:7077")  # this should be set to the spark master url
        .appName("spark-stats")
        .getOrCreate()
    )

    df = spark.read.parquet("s3a://s-laion/cc-proc-test/outputs/eadb7612-be05-4755-ba9b-c8e815f27fdc")

    print(f"Total number of rows: {df.count()}")

    df = df.dropDuplicates(["uid"])

    df.write.mode("overwrite").parquet("s3a://s-laion/cc-proc-test/dedup130k")

    df = spark.read.parquet("s3a://s-laion/cc-proc-test/dedup130k")

    print(f"Total number of rows after dedup: {df.count()}")



