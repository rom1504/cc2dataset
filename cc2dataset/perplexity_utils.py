from pathlib import Path

import sentencepiece
import kenlm

from typing import Iterable, Tuple, List
"""
Compute perplexity using a tokenizer + KenLM n-gram model
"""

try:
    from .normalizer_utils import text_norm
except ImportError:
    text_norm = None


# Default model name & model suffixes
DEFAULT_PREFIX = "kenlm"
KENLM_SUFFIX = ".arpa"
SP_SUFFIX = ".sp.model"


class SpTokenizer:
    """
    A SentencePiece tokenizer
    """
    def __init__(self, sp):
        self.sp = sp

    def __call__(self, text: str) -> str:
        return self.sp.encode_as_pieces(text)


class WsTokenizer:
    """
    A simple whitespace tokenizer
    """
    def __call__(self, text: str) -> str:
        return text.split()


def _scores(words: List[str], scores: Iterable[Tuple], debug: bool,
            word: bool = False) -> float:
    """
    Explicitly compute the total value by adding the scores for each n-gram
    """
    total = 0
    for i, (prob, length, oov) in enumerate(scores):
        total += prob
        if debug:
            s = i + (not word)
            print('{0:7.2f}  {1} {2}'.format(prob, length,
                                             ' '.join(words[s+1-length:s+1])))
            if oov:
                print('\t"{0}" is an OOV'.format(words[s]))
    if debug:
        print(f"{total:8.3f} TOTAL")
        num = len(words) + 1 if word else len(words) - 1
        print(f"# Perplexity: {10.0**(-total/num):8.4f}")

    return total


# ----------------------------------------------------------------------------

class PerplexityScorer:

    def __init__(self, kenlm_model: str, sp_model: str = None,
                 normalize: bool = False, debug: bool = False):
        """
          :param kenlm_model: filename holding the KenLM model
          :param sp_model: filename holding the SentencePiece tokenizer
          :param normalize: normalize input strings
          :param debug: print out all token perplexities
        """
        self.model = kenlm.Model(str(kenlm_model))
        self.norm = normalize and text_norm
        self.debug = debug
        if sp_model:
            tkn = sentencepiece.SentencePieceProcessor(str(sp_model))
            self.tkn = SpTokenizer(tkn)
        else:
            self.tkn = WsTokenizer()


    def __call__(self, text: str, **kwargs) -> float:
        """
        Compute the perplexity for a text buffer
        """
        if self.norm:
            text = text_norm(text)

        # Tokenize the string buffer
        tkns = self.tkn(text)

        # Join the tokens (since KenLM splits on whitespace) and compute score
        score = self.model.score(' '.join(tkns))

        # Compute the overall perplexity
        perplexity = 10.0**(-score / (len(tkns) + 1))

        if self.debug:
            all_scores = self.model.full_scores(' '.join(tkns), **kwargs)
            words = ['<s>'] + tkns + ['</s>']
            total = _scores(words, all_scores, debug=self.debug)
            if abs(total-score) > 1e-4:
                print("# Official score:", score)
                print("# Official perplexity:", perplexity)

        return perplexity


    def word(self, word: str) -> float:
        """
        Compute & print out the perplexity for a single word
        """
        if self.norm:
            word = text_norm(word)
        word_tkns = self.tkn(word)
        word = ' '.join(word_tkns)
        score = self.model.score(word, bos=False, eos=False)
        if self.debug:
            all_scores = self.model.full_scores(word, bos=False, eos=False)
            _scores(word_tkns, all_scores, debug=self.debug, word=True)
        return 10.0**(-score / (len(word_tkns) + 1))


# ---------------------------------------------------------------------


def load_perplexity_model(path: str, name_prefix: str = DEFAULT_PREFIX,
                          normalize: bool = False,
                          debug: bool = False) -> PerplexityScorer:
    """
    Create a PerplexityScorer object (containing a SentencePiece tokenizer and a
    KenLM n-gram perplexity model)
      :param path: base path where models are located
      :param prefix: filename prefix for model files
      :param normalize: normalize input strings
      :param debug: activate debug output
    """
    path = Path(path)

    # Find the filename of the KenLM model (either a trie or a bin model)
    klm = path / (name_prefix + KENLM_SUFFIX + '.bin')
    if not klm.is_file():
        klm = path / (name_prefix + KENLM_SUFFIX + '.trie')
    if not klm.is_file():
        raise Exception(f"Error: cannot find KenLM model file: {klm}")

    # Find the filename of a SentencePiece tokenizer
    spt = path / (name_prefix + SP_SUFFIX)
    if not spt.is_file():
        spt = None

    return PerplexityScorer(klm, spt, normalize=normalize, debug=debug)
