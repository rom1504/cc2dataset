# cc2imgcap
[![pypi](https://img.shields.io/pypi/v/cc2imgcap.svg)](https://pypi.python.org/pypi/cc2imgcap)
[![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/rom1504/cc2imgcap/blob/master/notebook/cc2imgcap_getting_started.ipynb)
[![Try it on gitpod](https://img.shields.io/badge/try-on%20gitpod-brightgreen.svg)](https://gitpod.io/#https://github.com/rom1504/cc2imgcap)

Easily convert common crawl to image caption set using pyspark.

Common crawl has [7.5M warc files](https://commoncrawl.org/the-data/get-started/). They provide links of the web.
This simple tool allows you to process one warc in about 20s and get image link along with the alt text.

This makes it possible to do the first step of building a dataset like [laion5B](https://laion.ai/blog/laion-5b/) in 50k cpu core hours.
That's $2k using aws EC2.

## Install

pip install cc2imgcap

## Python examples

Checkout these examples to call this as a lib:
* [single_warc_example.py](examples/single_warc_example.py)

## API

This module exposes a single function `hello_world` which takes the same arguments as the command line tool:

* **message** the message to print. (*required*)

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
