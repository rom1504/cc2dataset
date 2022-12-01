# cc2imgcap
[![pypi](https://img.shields.io/pypi/v/cc2imgcap.svg)](https://pypi.python.org/pypi/cc2imgcap)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rom1504/cc2imgcap/blob/master/notebook/cc2imgcap_getting_started.ipynb)
[![Try it on gitpod](https://img.shields.io/badge/try-on%20gitpod-brightgreen.svg)](https://gitpod.io/#https://github.com/rom1504/cc2imgcap)

Easily convert common crawl to image caption set using pyspark.

Common crawl has [5M wat files](https://commoncrawl.org/the-data/get-started/). They provide links of the web.
This simple tool allows you to process one warc in about 20s and get image link along with the alt text.

It also runs deduplication against url+text in order to save on output space and speed up the process.

This makes it possible to do the first step of building a dataset like [laion5B](https://laion.ai/blog/laion-5b/) in 30k cpu core hours. (`5*10^6*20/(3600)`)
That's `$1.2k` using aws EC2 (0.04$/core hour)

## What hardware to pick ?

`cpu128-dy-c6i-32xlarge` instances are advised. Spark stores the non duplicated first stage in local disk. They should be nvme drive for speed during deduplication. At this first stage, one wat takes about 20MB, so the total (over all workers) space must be more than 20MB times wat count. So for example for the whole CC, that means 100TB. So for example that can fit in 150 instances with 1TB nvme drive each. 150 instances of 128 cores is 19200 cores so the whole processing takes 2h. Less instances with bigger hard drives can work too. It's also a possibility to do the processing in multiple pieces if temporary disk space is an issue by specifying `--multipart`.

## Install

pip install cc2imgcap

## Python examples

Checkout these examples:
* [run_on_spark.py](examples/run_on_spark.py) it shows how to bring your own spark session

If you have a slurm cluster, refer to https://gist.github.com/rom1504/67ada3dedbecc113ae2dbdfd9c642d83 to start a spark cluster there.

## API

This module exposes a single function `cc2imgcap` which takes the same arguments as the command line tool:
* **output_path** the output path, should probably start with s3://. (*required*)
* **wat_index_count** the number of wat index files to read, can be None for all. (*default 1*)
* **wat_count** the number of wat files to read, can be None for all, will randomly subsample if present. (*default 100*)
* **master** the spark master url. (*default local*)
* **num_cores** the number of cores of each spark executor. (*default 128*)
* **mem_gb** the memory of each spark executor. (*default 256*)
* **multipart** runs the processing of the specified number of parts, merge at the end (*default None*)

## For development

Either locally, or in [gitpod](https://gitpod.io/#https://github.com/rom1504/cc2imgcap) (do `export PIP_USER=false` there)

Setup a virtualenv:

```
python3 -m venv .env
source .env/bin/activate
pip install -e .
```

to run tests:
```
pip install -r requirements-test.txt
```
then 
```
make lint
make test
```

You can use `make black` to reformat the code

`python -m pytest -x -s -v tests -k "dummy"` to run a specific test
