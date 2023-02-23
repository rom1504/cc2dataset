
from os import environ
import pytest

from cc2dataset import kenlm as mod

DEFAULT_CACHEDIR = '/tmp'


def test_perplexity_scorer():
    cache_dir = environ.get("TMPDIR", DEFAULT_CACHEDIR)
    model = mod.PerplexityScorer(cache_dir=cache_dir)
    result = model("this is an example", prefix="perp/")
    print(result)
    assert list(result) ==  ['perp/ccnet/wikipedia', 'perp/ontocord/riverbed_kenlm']
    assert result['perp/ccnet/wikipedia'] == pytest.approx(5394.3)
    assert result['perp/ontocord/riverbed_kenlm'] == pytest.approx(30.2)
