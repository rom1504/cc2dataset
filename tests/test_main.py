import pytest
from cc2imgcap import hello_world


@pytest.mark.parametrize("message", ["hello", "world"])
def test_hello_world(message):
    hello_world(message)
