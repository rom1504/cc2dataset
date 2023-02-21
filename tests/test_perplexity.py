
from pathlib import Path

import tempfile
import pytest
from unittest.mock import Mock

from cc2dataset import perplexity_utils as mod


def patch_objects(monkeypatch):
    """
    Monkey-patch the KenLM and SentencePiece classes
    """
    mock_spp = Mock()
    mock_spp.encode_as_pieces = lambda s: s.split()

    mock_sp = Mock()
    mock_sp.SentencePieceProcessor(return_value=mock_spp)

    mock_klm = Mock()
    mock_klm.score = Mock(return_value=0.1)
    mock_klm.full_scores = Mock(return_value=[(0.1, 2, False)])

    mock_kl = Mock()
    mock_klm_constructor = Mock(return_value=mock_klm)
    mock_kl.Model = mock_klm_constructor

    monkeypatch.setattr(mod, 'sentencepiece', mock_sp)
    monkeypatch.setattr(mod, 'kenlm', mock_kl)
    return mock_klm.score


def test_perplexity(monkeypatch):

    with tempfile.TemporaryDirectory() as d:

        # Create a fake empty KenLM file
        dummy = Path(d) / (mod.DEFAULT_PREFIX + mod.KENLM_SUFFIX + ".bin")
        open(dummy, "w").close()

        # Monkey patch KenLM & SentencePiece
        scorer = patch_objects(monkeypatch)

        model = mod.load_perplexity_model(d)
        v = model("this is an example")

        assert v == pytest.approx(0.9549925)
        assert scorer.called_once()
        assert scorer.call_args[0] == ("this is an example",)
