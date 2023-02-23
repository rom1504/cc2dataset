#@title KenLM code
"""
Copyright, 2021-2022 Ontocord, LLC, and other authors of Muliwai, All rights reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
# from https://github.com/piisa/muliwai/blob/main/kenlm_manager.py
# which is based Eduardo Gonzalez Ponferrada/edugp's repo: https://huggingface.co/edugp/kenlm/blob/main/model.py which is under the Apache 2 License
# which is also based on https://github.com/facebookresearch/cc_net/ which is under the MIT License
# thank you edugp!!
import os
import re
import unicodedata
from typing import Dict
import warnings
from filelock import FileLock
import tempfile
import gzip

import kenlm
import sentencepiece
from huggingface_hub import cached_download, hf_hub_url
try:
  import transformers
except:
  os.system("pip install transformers sentencepiece")
from transformers import AutoTokenizer

from .filtering import *
from .cjk import *
from .char_manager import *

try:
  if mt5_tokenizer is None: pass
except:
  mt5_tokenizer = AutoTokenizer.from_pretrained("google/mt5-small")
mt5_underscore = "▁"


## cache the models in main memory so we don't have to load them over and over
kenlm_models = {
    'ccnet/wikipedia': {},
    'edugp/oscar': {},
    'edugp/mc4': {},
    'ontocord/riverbed_kenlm': {}
}

#NOTE: If you want to use the default cc_net kenlm wikipedia models, you will need to download them. You can manually download per the below or copy them from a saved dir.
#Alternately, they will be downloaded automatically using the load_kenlm_model function.

#see https://github.com/facebookresearch/cc_net/blob/main/Makefile. These are default models if there aren't any from edugp
ccnet_langs=set("af,ar,az,be,bg,bn,ca,cs,da,de,el,en,es,et,fa,fi,fr,gu,he,hi,hr,hu,hy,id,is,it,ja,ka,kk,km,kn,ko,lt,lv,mk,ml,mn,mr,my,ne,nl,no,pl,pt,ro,ru,uk,zh".split(","))
def download_ccnet_sp_kenlm_models(lang, default_kenlm_wikipedia="./kenlm_ccnet_wikipedia_models"):
  os.system(f"mkdir -p {default_kenlm_wikipedia}")
  if not os.path.exists(f"{default_kenlm_wikipedia}/{lang}.arpa.bin"):
    with FileLock(f"{default_kenlm_wikipedia}/{lang}.arpa.bin.lock"):
      print (f"downloading {default_kenlm_wikipedia}/{lang}.arpa.bin")
      os.system(f"wget -c  -P {default_kenlm_wikipedia} http://dl.fbaipublicfiles.com/cc_net/lm/{lang}.arpa.bin")
  if not os.path.exists(f"{default_kenlm_wikipedia}/{lang}.sp.model"):
    with FileLock(f"{default_kenlm_wikipedia}/{lang}.sp.model.lock"):
      print (f"downloading {default_kenlm_wikipedia}/{lang}.sp.model")
      os.system(f"wget -c  -P {default_kenlm_wikipedia} http://dl.fbaipublicfiles.com/cc_net/lm/{lang}.sp.model")

def get_kenlm_models_from_savedir( default_kenlm_wikipedia="./kenlm_ccnet_wikipedia_models", save_dir="/content/drive/Shareddrives/LAION/kenlm_ccnet_wikipedia_models"):
  if not os.path.exists(default_kenlm_wikipedia):
    with FileLock(f"{default_kenlm_wikipedia}.lock"):
        print ("copying kenlm models")
        os.system(f"cp -rf {save_dir} {default_kenlm_wikipedia}")

# TODO figure out actual numbers for riverbed_kenlm. Check if there are variations b/c of
# gender, so we would have at least two patterns.
public_figure_kenlm_cutoff_map = {
    'en': {'wikipedia': [{'cutoff': 500, 'pattern': "{} (born"}],  # in wikipedia, you often have: Lincoln (born .... )
           'oscar': [{'cutoff': 500, 'pattern': "{} was born"}],
           },
    'yo': {'wikipedia': [{'cutoff': 400, 'pattern': "{} ni a bi lori"}],
           'oscar': [{'cutoff': 400, 'pattern': "{} ni a bi lori"}],
           },
    'zu': {'wikipedia': [{'cutoff': 400, 'pattern': "{} wazalwa ngo"}],
           'oscar': [{'cutoff': 400, 'pattern': "{} wazalwa ngo"}],
           'mc4': [{'cutoff': 400, 'pattern': "{} wazalwa ngo"}],  # for now, we are using the mc4 model for zu and ig
           },
    'sn': {'wikipedia': [{'cutoff': 500, 'pattern': "{} akazvarwa"}],
           'oscar': [{'cutoff': 500, 'pattern': "{} akazvarwa"}],
           },
    'st': {'wikipedia': [{'cutoff': 500, 'pattern': "{} o hlahile ka"}],
           'oscar': [{'cutoff': 500, 'pattern': "{} o hlahile ka"}],
           },
    'ny': {'wikipedia': [{'cutoff': 500, 'pattern': "{} anabadwa pa"}],
           'oscar': [{'cutoff': 500, 'pattern': "{} anabadwa pa"}],
           },
    'xh': {'wikipedia': [{'cutoff': 500, 'pattern': "{} wazalwa ngo"}],
           'oscar': [{'cutoff': 500, 'pattern': "{} wazalwa ngo"}],
           },
    'sw': {'wikipedia': [{'cutoff': 500, 'pattern': "{} alizaliwa tarehe"}],
           'oscar': [{'cutoff': 500, 'pattern': "{} alizaliwa tarehe"}],
           },
    'ig': {'wikipedia': [{'cutoff': 300, 'pattern': "{} amụrụ"}],
           'oscar': [{'cutoff': 300, 'pattern': "{} amụrụ"}],
           'mc4': [{'cutoff': 300, 'pattern': "{} amụrụ"}],
           },
    'ar': {'wikipedia': [{'cutoff': 600, 'pattern': "ولد {} من"}],
           'oscar': [{'cutoff': 600, 'pattern': "ولد {} من"}]
           },
    'zh': {'wikipedia': [{'cutoff': 500, 'pattern': "{}生於"}],
           'oscar': [{'cutoff': 500, 'pattern': "{}生於"}]
           },
    'vi': {'wikipedia': [{'cutoff': 500, 'pattern': "{} sinh ra"},
                         {'cutoff': 500, 'pattern': "{} sáng lập"}],
           'oscar': [{'cutoff': 450, 'pattern': "{} sinh ra"},
                     {'cutoff': 450, 'pattern': "{} sáng lập"}],
           },
    'hi': {'wikipedia': [{'cutoff': 500, 'pattern': "{} का जन्म ए"}],
           'oscar': [{'cutoff': 500, 'pattern': "{} का जन्म ए"}],
           },
    'ur': {'wikipedia': [{'cutoff': 500, 'pattern': "{} پیدا ہوا"}],
           'oscar': [{'cutoff': 500, 'pattern': "{} پیدا ہوا"}],
           },
    'id': {'wikipedia': [{'cutoff': 500, 'pattern': "{} lahir"}],
           'oscar': [{'cutoff': 500, 'pattern': "{} lahir"}],
           },
    'bn': {'wikipedia': [{'cutoff': 500, 'pattern': "{} জন্ম"}],
           'oscar': [{'cutoff': 500, 'pattern': "{} জন্ম"}],
           }
}


def train_kenlm_model(model_name, data_files,  parse_file=None, min_num_tokens=5, do_collapse_values=True, lmplz_loc = "./riverbed/bin/lmplz", build_binary_loc = "./riverbed/bin/build_binary", tokenizer=None, do_lowercase=True, ngram_score=0.8, special_char_score=0.28, flaggedword_score=0.08, remove_accents=False):
  global mt5_tokenizer
  if tokenizer is None: tokenizer = mt5_tokenizer
  if lmplz_loc != "./riverbed/bin/lmplz" and not os.path.exists("./lmplz"):
        os.system(f"cp {lmplz_loc} ./lmplz")
        lmplz = "./lmplz"
  else:
        lmplz = lmplz_loc
  os.system(f"chmod u+x {lmplz}")
  if build_binary_loc != "./riverbed/bin/build_binary" and not os.path.exists("./build_binary"):
        os.system(f"cp {build_binary_loc} ./build_binary")
        build_binary = "./build_binary"
  else:
        build_binary = build_binary_loc
  os.system(f"chmod u+x {build_binary}")  
  temp_name = tempfile._get_default_tempdir() + "/" + next(tempfile._get_candidate_names()) + ".txt"
  if parse_file is None: parse_file = temp_name
  with open(parse_file, "w", encoding="utf8") as out:  
    for filename in data_files:
      if filename.endswith(".gz"):
        fin = gzip.open(filename)
      else:
        fin = open(filename, "rb")
      for line in fin:
        line = line.decode().strip()
        lang = cjk_detect(line)
        if not lang: lang = "en"
        score = get_ngram_score(lang, line)
        if score >= ngram_score:
          continue
        score = get_special_char_score(lang, line)
        if score >= special_char_score:
          continue
        score = get_flaggedword_score(lang, line)
        if score >= flaggedword_score:
          continue
        line = KenlmModel.normalize(
            line,
            accent=remove_accents,
            lowercase=do_lowercase,
            numbers = True,
            punct = 1,
            do_tokenize = True,
            tokenizer = tokenizer,
            do_normalize_spacing_for_tok = True
        )
        out.write(line+"\n")
  if do_collapse_values:
      os.system(f"./{lmplz} --collapse_values  --discount_fallback  --skip_symbols -o 5 --prune {min_num_tokens}  --arpa {model_name}.arpa <  {parse_file}") ##
  else:
      os.system(f"./{lmplz}  --discount_fallback  --skip_symbols -o 5 --prune {min_num_tokens}  --arpa {model_name}.arpa <  {parse_file}") ##
  os.system(f"rm {temp_name}")
  os.sytem(f"./{build_binary} -i {model_name}.arpa {model_name}.bin")        

def load_kenlm_model(
        language: str = "*",
        pretrained_models: list = ['ontocord/riverbed_kenlm'],
        store_model: bool = True,
        cache_dir: str = None,
        default_kenlm_wikipedia: str = "./kenlm_ccnet_wikipedia_models"
) -> dict:
    """
    Load all supported kenlm model for source language. Consider if we want to use an LRU.
    TODO: Incorporate OSCAR kenlm models. They are quite big, and we still need patterns and cutoffs.
    """
    assert len(pretrained_models) <= len(
        kenlm_models), 'Total of number kenlm models loads larger than supported kenlm models'
    all_models = {}
    model_files = ["arpa.bin", "sp.model", ] # "sp.vocab"
    # cache to dir
    if cache_dir is None:
        cache_dir = os.path.expanduser('~') + "/.cache"
    if language is None: language = '*'
    # check if pretrain model exist
    for model_name in pretrained_models:
        print("MODEL", model_name)
        if language in kenlm_models[model_name]:
            all_models[model_name] = kenlm_models[model_name][language]
        elif "wikipedia" in model_name  and os.path.exists(f"{default_kenlm_wikipedia}/{language}.arpa.bin"):
            model = KenlmModel(default_kenlm_wikipedia, language, do_normalize_spacing_for_tok=True)
            all_models[model_name] = model
            if store_model:
              kenlm_models[model_name][language] = model
        elif "wikipedia" in model_name  and language in ccnet_langs:
            download_ccnet_sp_kenlm_models(language, default_kenlm_wikipedia)
            model = KenlmModel(default_kenlm_wikipedia, language, do_normalize_spacing_for_tok=True)
            all_models[model_name] = model
            if store_model:
              kenlm_models[model_name][language] = model
        elif model_name not in kenlm_models:
            warnings.warn(f"{model_name} pretrained model is not supported!")
        else:
            os.system(f"mkdir -p {cache_dir}/{model_name}")
            found = True
            if language == '*':
              if not os.path.exists(f"{cache_dir}/{model_name}/arpa.bin"):
                    try:
                        print (f"{cache_dir}/{model_name}/arpa.bin")
                        file_url = hf_hub_url(repo_id=model_name,
                                              filename=f"arpa.bin")
                        file = cached_download(file_url)
                        os.system(f"ln -s {file} {cache_dir}/{model_name}/arpa.bin")
                    except:
                        warnings.warn(f'could not find model ontocord/riverbed_kenlm/arpa.bin. will stop searching...')
                        found = False
            else:        
              for model_file in model_files:
                if not os.path.exists(f"{cache_dir}/{model_name}/{language}.{model_file}"):
                    try:
                        repo_id = "/".join(model_name.split("/")[:1])
                        model_subtype = "/".join(model_name.split("/")[1:])
                        print (f"loading {model_name}/{language}.{model_file}")
                        file_url = hf_hub_url(repo_id=repo_id,
                                              filename=f"{model_subtype}/{language}.{model_file}")
                        file = cached_download(file_url)
                        os.system(f"ln -s {file} {cache_dir}/{model_name}/{language}.{model_file}")
                    except:
                        warnings.warn(f'could not find model {language}.{model_file}. will stop searching...')
                        found = False
                        break
            if found:
                model = KenlmModel(f"{cache_dir}/{model_name}", language)
                all_models[model_name] = model
                if store_model:
                    kenlm_models[model_name][language] = model
    return all_models


# TODO: refactor code in the faker_extensions with this code
def check_for_common_name(
        language: str = "en",
        pretrained_models: list = ['wikipedia'],
        name: str = None,
        verbose: bool = False,
        kenlm_models=None,
        return_score=False,
):
    """
    Check if a name is a public figure or a very common name
    """
    # load all kenlm models and cutoff patterns
    if kenlm_models is None:
        kenlm_models = load_kenlm_model(language, pretrained_models)
    public_patterns = public_figure_kenlm_cutoff_map.get(language, public_figure_kenlm_cutoff_map.get('en'))
    for model_name, model in kenlm_models.items():
        for pattern in public_patterns.get(model_name, public_patterns.get('wikipedia')):
            test_name = pattern['pattern'].format(name)
            score = model.get_perplexity(test_name)
            if score < pattern['cutoff']:
                #if verbose:
                #    print(name, score)
                if return_score:
                    return True, score, pattern['cutoff']
                return True
    if return_score:
        return False, 0.0, 0.0
    return False


### Edugp code

class SentencePiece:
    def __init__(
            self,
            model: str,
    ):
        super().__init__()
        self.sp = sentencepiece.SentencePieceProcessor()
        self.sp.load(str(model))

    def tokenize(self, text: dict) -> dict:
        tokenized = self.sp.encode_as_pieces(text)
        return " ".join(tokenized)

#see https://github.com/facebookresearch/cc_net/blob/main/cc_net/text_normalizer.py
class KenlmModel:
    digit_re: re.Pattern = re.compile(r"\d")
    unicode_punct: Dict[str, str] = {
        "，": ",",
        "。": ".",
        "、": ",",
        "„": '"',
        "”": '"',
        "“": '"',
        "«": '"',
        "»": '"',
        "１": '"',
        "」": '"',
        "「": '"',
        "《": '"',
        "》": '"',
        "´": "'",
        "∶": ":",
        "：": ":",
        "？": "?",
        "！": "!",
        "（": "(",
        "）": ")",
        "；": ";",
        "–": "-",
        "—": " - ",
        "．": ". ",
        "～": "~",
        "’": "'",
        "…": "...",
        "━": "-",
        "〈": "<",
        "〉": ">",
        "【": "[",
        "】": "]",
        "％": "%",
        "►": "-",
    }
    unicode_punct_re = re.compile(f"[{''.join(unicode_punct.keys())}]")
    non_printing_chars_re = re.compile(
        f"[{''.join(map(chr, list(range(0, 32)) + list(range(127, 160))))}]"
    )
    kenlm_model_dir = None
    sentence_piece_model_dir = None

    # TODO: we are not doing the sacremoses tokenizer to get put spaces between escaped chars 
    # but consider whether we should do this for the ccnet models 
    # https://github.com/facebookresearch/cc_net/blob/bda555bd1cf1ee2e0b925363e62a61cd46c8b60d/cc_net/tokenizer.py#L23
    # does it make a difference?
    def __init__(
            self,
            model_name: str=None,
            language: str=None,
            lowercase: bool = False,
            remove_accents: bool = False,
            normalize_numbers: bool = True,
            punctuation: int = 1,
            do_normalize_spacing_for_tok: bool = False,
            tokenizer=None,
            model_path: str=None,
    ):
        print (model_name)
        self.model_name = model_name
        if model_path is not None:
          self.model = kenlm.Model(model_path)
        elif "riverbed" in model_name:
          self.model = kenlm.Model(os.path.join(self.model_name, f"arpa.bin"))
          tokenizer = mt5_tokenizer
        else:
          self.model = kenlm.Model(os.path.join(self.model_name, f"{language}.arpa.bin"))
        if tokenizer is None:
          self.tokenizer = SentencePiece(os.path.join(self.model_name, f"{language}.sp.model"))
        else:
          self.tokenizer = tokenizer
        self.do_normalize_spacing_for_tok = do_normalize_spacing_for_tok
        self.accent = remove_accents
        self.lowercase = lowercase
        self.numbers = normalize_numbers
        self.punct = punctuation
        self.language = language

    @classmethod
    def from_pretrained(
            cls,
            model_name: str,
            language: str='*',
    ):
        load_kenlm_model(
            language = language,
            pretrained_models =[model_name],
            )
        return cls(
            model_name = model_name,
            language = language,
            lowercase = "edugp" not in model_name,
            remove_accents = language  in {"en", "my"},
            do_normalize_spacing_for_tok = "edugp" not in model_name,
        )

    def pp(self, log_score, length):
        return 10.0 ** (-log_score / length)

    # Tokenize (after normalizing): See https://github.com/facebookresearch/cc_net/blob/bda555bd1cf1ee2e0b925363e62a61cd46c8b60d/cc_net/mine.py#L352 for full pipeline        
    def get_perplexity(self, doc: str):
        doc = self.normalize(
                doc,
                accent=self.accent,
                lowercase=self.lowercase,
                numbers=self.numbers,
                punct=self.punct,
                tokenizer=self.tokenizer,
                do_tokenize=True,
                do_normalize_spacing_for_tok=self.do_normalize_spacing_for_tok
            )
        doc_log_score, doc_length = 0, 0
        for line in doc.split("\n"):
            log_score = self.model.score(line)
            length = len(line.split()) + 1
            doc_log_score += log_score
            doc_length += length
        return round(self.pp(doc_log_score, doc_length), 1)
    
    @staticmethod
    def normalize(
            line: str,
            accent: bool = False,
            lowercase: bool = True,
            numbers: bool = True,
            punct: int = 1,
            do_tokenize: bool = True,
            tokenizer = None,
            do_normalize_spacing_for_tok: bool = True
    ) -> str:
        line = line.strip()
        if not line:
            return line
        if lowercase:
            line = line.lower()
        if accent:
            line = KenlmModel.strip_accents(line)
        if numbers:
            line = KenlmModel.digit_re.sub("0", line)
        if punct == 1:
            line = KenlmModel.replace_unicode_punct(line)
        elif punct == 2:
            line = KenlmModel.remove_unicode_punct(line)
        line = KenlmModel.remove_non_printing_char(line)
        if do_tokenize:
          assert tokenizer is not None
          if do_normalize_spacing_for_tok:
            line = KenlmModel.normalize_spacing_for_tok(line)
          line = tokenizer.tokenize(line)
          line  = " ".join(" ".join(line).replace(mt5_underscore, " ").split())
          for w in punc_char:
            line = line.replace(" "+w, w)
        return line

    @staticmethod
    def normalize_spacing_for_tok(text: str, language: str = "en") -> str:
      res = (
          text.replace("\r", "")
          # remove extra spaces
          .replace("(", " (")
          .replace(")", ") ")
          .replace(" +", " ")
      )
      res = re.sub(r"\) ([\.\!\:\?\;\,])", r"\)\1", res)
      res = res.replace("( ", "(").replace(" )", ")")
      res = re.sub(r"(\d) \%", r"\1\%", res)
      res = res.replace(" :", ":").replace(" ;", ";")
      res = res.replace("`", "'").replace("''", ' " ')

      res = (
          res.replace("„", '"')
          .replace("“", '"')
          .replace("”", '"')
          .replace("–", "-")
          .replace("—", " - ")
          .replace(" +", " ")
          .replace("´", "'")
          .replace("([a-z])‘([a-z])", r"\1'\2/")
          .replace("([a-z])’([a-z])", r"\1'\2/")
          .replace("‘", '"')
          .replace("‚", '"')
          .replace("’", '"')
          .replace("''", '"')
          .replace("´´", '"')
          .replace("…", "...")
          # French quotes
          .replace(" « ", ' "')
          .replace("« ", '"')
          .replace("«", '"')
          .replace(" » ", '" ')
          .replace(" »", '"')
          .replace("»", '"')
          # handle pseudo-spaces
          .replace(" %", "%")
          .replace("nº ", "nº ")
          .replace(" :", ":")
          .replace(" ºC", " ºC")
          .replace(" cm", " cm")
          .replace(" ?", "?")
          .replace(" !", "!")
          .replace(" ;", ";")
          .replace(", ", ", ")
          .replace(" +", " ")
          .replace("．", ". ")
      )
      # English "quotation," followed by comma, style
      if language == "en":
          res = re.sub(r"\"([,\.]+)", r"\1\"", res)
      # Czech is confused
      elif language == "cs" or language == "cz":
          pass
      # German/Spanish/French "quotation", followed by comma, style
      else:
          res = res.replace(',"', '",')
          res = re.sub(
              r"(\.+)\"(\s*[^<])", r"\"\1\2", res
          )  # don't fix period at end of sentence

      if (
          language == "de"
          or language == "es"
          or language == "cz"
          or language == "cs"
          or language == "fr"
      ):
          res = re.sub(r"(\d) (\d)", r"\1,\2", res)
      else:
          res = re.sub(r"(\d) (\d)", r"\1.\2", res)
      return res

    @staticmethod
    def strip_accents(line: str) -> str:
        """Strips accents from a piece of text."""
        nfd = unicodedata.normalize("NFD", line)
        output = [c for c in nfd if unicodedata.category(c) != "Mn"]
        if len(output) == line:
            return line
        return "".join(output)

    @staticmethod
    def replace_unicode_punct(text: str) -> str:
        return "".join(KenlmModel.unicode_punct.get(c, c) for c in text)

    @staticmethod
    def remove_unicode_punct(text: str) -> str:
        """More aggressive version of replace_unicode_punct but also faster."""
        return KenlmModel.unicode_punct_re.sub("", text)

    @staticmethod
    def remove_non_printing_char(text: str) -> str:
        return KenlmModel.non_printing_chars_re.sub("", text)
    
    def check_common_name(self, name: str, return_score: bool = False):
        """
        Check if a name is a common name.

        :param name: Name to check.
        :param return_score: If True, return the score of the name and cutoff threshold of the pattern.
        :return: True if name is a common name, False otherwise.
        """
        public_patterns = public_figure_kenlm_cutoff_map.get(self.language, public_figure_kenlm_cutoff_map.get('en'))
        model_name = self.model_name.split("/")[-1]
        for pattern in public_patterns.get(model_name, public_patterns.get('wikipedia')):
            test_name = pattern['pattern'].format(name)
            score = self.get_perplexity(test_name)
            if score < pattern['cutoff']:
                if return_score:
                    return True, score, pattern['cutoff']
                return True
        if return_score:
            return False, 0.0, 0.0
        return False
