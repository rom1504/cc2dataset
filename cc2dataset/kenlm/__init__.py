"""
Wrapper to compute perplexity for KenLM models
"""

from typing import List, Dict, Union, Tuple

from .kenlm_manager import load_kenlm_model

DEFAULT_MODELS = [
    ('en', 'ccnet/wikipedia'),
    ('*', 'ontocord/riverbed_kenlm')
]


class PerplexityScorer:

    def __init__(self, models: List[Tuple] = None, cache_dir: str = None):
        """
        Create object and load all passed models.
          :param models: list of models to load. If no model is passed, it
             will load a dfault list
          :param cache_dir: directory where to store downloaded modelsxs
        """
        if models is None:
            models = DEFAULT_MODELS
        self.models = {m[1]: load_kenlm_model(m[0], pretrained_models=[m[1]],
                                              cache_dir=cache_dir)[m[1]]
                       for m in models}


    def __call__(self, text: str, model: str = None,
                 prefix: str = "") -> Union[float, Dict[str, float]]:
        """
          :param text: document to compute perplexity from
          :param model: specific perplexity model
          :param prefix: prefix for the output dictionary keys
        If given a specific model, return the perplexity for it
        If not, return a dictionary with the perplexity for all loaded models
        """
        if model:
            return self.models[model].get_perplexity(text)
        else:
            return {prefix + n: m.get_perplexity(text)
                    for n, m in self.models.items()}
