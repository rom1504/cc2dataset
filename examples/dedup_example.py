# Example to deduplicate the result

from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext


if __name__ == "__main__":
    spark = (
        SparkSession.builder.config("spark.submit.deployMode", "client")
        .config("spark.executor.memory", "48GB")
        .config("spark.executor.cores", "128")  # this can be set to the number of cores of the machine
        .config("spark.task.cpus", "1")
        .config("spark.executor.memoryOverhead", "16GB")
        .config("spark.task.maxFailures", "2")
        .config('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.1,org.apache.spark:spark-hadoop-cloud_2.13:3.3.1')
         # change to the appropriate auth method, see https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
        .config('spark.hadoop.fs.s3a.aws.credentials.provider', 'com.amazonaws.auth.InstanceProfileCredentialsProvider')
        .config('fs.s3a.threads.max', '96')
        .config('fs.s3a.connection.maximum', '96')
        .config('fs.s3a.block.size', '1024')
        .config('fs.s3a.fast.upload.buffer', 'bytebuffer')
        .config('spark.hadoop.fs.s3a.bucket.all.committer.magic.enabled', 'true')
        .master("spark://cpu128-dy-c6i-32xlarge-11:7077")  # this should be set to the spark master url
        .appName("spark-stats")
        .getOrCreate()
    )

    df = spark.read.parquet("s3a://s-laion/cc-proc-test/outputs/a8d6eb4f-e643-4f28-aa70-2ea288c6f2fb")

    print(f"Total number of rows: {df.count()}")

    df = df.dropDuplicates(["uid"])

    df.write.mode("overwrite").parquet("s3a://s-laion/cc-proc-test/dedup2048")

    df = spark.read.parquet("s3a://s-laion/cc-proc-test/dedup2048")

    print(f"Total number of rows after dedup: {df.count()}")



