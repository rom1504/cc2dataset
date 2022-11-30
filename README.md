# cc2imgcap
[![pypi](https://img.shields.io/pypi/v/cc2imgcap.svg)](https://pypi.python.org/pypi/cc2imgcap)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rom1504/cc2imgcap/blob/master/notebook/cc2imgcap_getting_started.ipynb)
[![Try it on gitpod](https://img.shields.io/badge/try-on%20gitpod-brightgreen.svg)](https://gitpod.io/#https://github.com/rom1504/cc2imgcap)

Easily convert common crawl to image caption set using pyspark.

Common crawl has [7.5M warc files](https://commoncrawl.org/the-data/get-started/). They provide links of the web.
This simple tool allows you to process one warc in about 40s and get image link along with the alt text.

This makes it possible to do the first step of building a dataset like [laion5B](https://laion.ai/blog/laion-5b/) in 100k cpu core hours.
That's $4k using aws EC2.

## Install

pip install cc2imgcap

## Python examples

Checkout these examples:
* [run_on_spark.py](examples/run_on_spark.py) it shows how to bring your own spark session

## API

This module exposes a single function `cc2imgcap` which takes the same arguments as the command line tool:
* **output_path** the output path, should probably start with s3://. (*required*)
* **wat_index_count** the number of wat index files to read, can be None for all. (*default 1*)
* **wat_count** the number of wat files to read, can be None for all, will randomly subsample if present. (*default 100*)

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
