# cc2dataset
[![pypi](https://img.shields.io/pypi/v/cc2dataset.svg)](https://pypi.python.org/pypi/cc2dataset)
[![Try it on gitpod](https://img.shields.io/badge/try-on%20gitpod-brightgreen.svg)](https://gitpod.io/#https://github.com/rom1504/cc2dataset)

Easily convert common crawl to a dataset of caption and document. Image/text Audio/text Video/text, ...

Common crawl has [5M wat files](https://commoncrawl.org/the-data/get-started/). They provide links of the web.
This simple tool allows you to process one warc in about 50s and get documents link along with the alt text.

It also runs deduplication against url+text in order to save on output space and speed up the process.

This makes it possible to do the first step of building a dataset like [laion5B](https://laion.ai/blog/laion-5b/) in 70k cpu core hours. (`5*10^6*50/(3600)`)
That's `$2.8k` using aws EC2 (0.04$/core hour)

If you believe in making reusable tools to make data easy to use for ML and you would like to contribute, please join the [DataToML](https://discord.gg/ep8yUUtCnp) chat.

## Intended usage

This tool produces a collection of link + caption. It is meant as the stage 1 of creating a dataset. It does deduplication and as minimal as possible filtering (does it look like an url / is the caption non empty).

This produces a large quantity of raw data that can then be further filtered by appropriate techniques.
An example of stage 2 can be to estimate the similarity between (link, text) with a model such as CLIP. This may reduce the quantity of data by a factor of up to 100x depending on the chosen threshold.

## What hardware to pick ?

CC is big and located at s3 us east 1, so it makes a lot of sense in term of network to use machines located in the same place.

`cpu128-dy-c6i-32xlarge` instances are advised. Spark stores the non duplicated first stage in local disk. They should be nvme drive for speed during deduplication. At this first stage, one wat takes about 20MB, so the total (over all workers) space must be more than 20MB times wat count. So for example for the whole CC, that means 100TB. So for example that can fit in 150 instances with 1TB nvme drive each. 150 instances of 128 cores is 19200 cores so the whole processing takes 2h. Less instances with bigger hard drives can work too. It's also a possibility to do the processing in multiple pieces if temporary disk space is an issue by specifying `--multipart`.

## Document type

This tool support extracting several documents from CC:

* image/text: 88B after dedup
* image/text even with empty text: 229B after dedup
* audio/text: 500M after dedup
* text doc : 5B after dedup
* video/text: 300M after dedup

They can be selected with eg `--document_type audio`.
You may experiment with more document kinds by running `python example single_warc_example.py` and exploring the resulting output.parquet.

## Install

pip install cc2dataset

## Python examples

Checkout these examples:
* [run_on_spark.py](examples/run_on_spark.py) it shows how to bring your own spark session

If you have a slurm cluster, refer to https://gist.github.com/rom1504/67ada3dedbecc113ae2dbdfd9c642d83 to start a spark cluster there.

## API

This module exposes a single function `cc2dataset` which takes the same arguments as the command line tool:
* **output_path** the output path, should probably start with s3://. The output will be written to this path sufixed by the date (*required*)
* **wat_index_count** the number of wat index files to read, can be None for all. (*default 1*)
* **wat_count** the number of wat files to read, can be None for all, will randomly subsample if present. (*default 100*)
* **master** the spark master url. (*default local*)
* **num_cores** the number of cores of each spark executor. (*default 128*)
* **mem_gb** the memory of each spark executor. (*default 256*)
* **multipart** runs the processing of the specified number of parts, merge at the end (*default None*)
* **shuffle** randomly shuffle the output right before saving (*default True*)
* **resume** the specific path of the output to resume (*default None*)
* **spark_builder** a function that create a spark session, None will default to the built-in methods (*default None*)
* **document_type** the kind of document to extract (*default image*)
* **source_cc_protocol** get common crawl from http or s3 (*default s3*)

## For development

Either locally, or in [gitpod](https://gitpod.io/#https://github.com/rom1504/cc2dataset) (do `export PIP_USER=false` there)

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


## Thanks

* [Vaishaal](https://github.com/Vaishaal) for providing the initial CC parsing code with efficient libraries
* [rvencu](https://github.com/rvencu) for optimizing the cc [parsing code](https://github.com/rvencu/crawlingathome-gpu-hcloud) for laion5B on which the idea of this package is based on
