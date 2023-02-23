#@title Basic Filtering Code
"""
Copyright, 2021-2022 Ontocord, LLC, All rights reserved.
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

#adapted from https://github.com/piisa/muliwai/blob/main/preprocess_manager.py, and

from .stopwords import all_stopwords
from .flagged_words import flagged_words
from .char_manager import *
from .cjk import *

from collections import Counter
def get_ngram(text, window_size=3, lang="en"):
  if lang_is_cjk(lang):
    tokens = text
    ret= ["".join(tokens[i : i + window_size])   for i in range(len(tokens) - window_size)]
  else:
    tokens = text.split()
  ret= [" ".join(tokens[i : i + window_size])   for i in range(len(tokens) - window_size)]
  return Counter(ret)

def get_ngram_score(lang, text, window_size=3):
  aHash = get_ngram(text, window_size, lang)
  text_len = text.count(" ")+1
  for key in list(aHash.keys()):
    aHash[key] = aHash[key]/text_len
  if not aHash: return 0.0
  return aHash.most_common(1)[0][1]
  
def get_special_char_score (lang, text, special_characters_default=None):
  global junk
  if len(text) == 0: return 1
  #TODO: do we want to do any lang specific special_chars?
  if special_characters_default is None: special_characters_default = junk
  return len([a for a in text if a in special_characters_default])/len(text)


lang_2_max_stopword_len = dict([(lang, max(s.count(" ")+1 if not lang_is_cjk(lang) else len(s) for s in arr)) for lang, arr in all_stopwords.items()])

def get_stopword_score(lang, text, max_word_len=3, cjk_scale=1.5):
    is_cjk = lang_is_cjk(lang)
    stopwords =  all_stopwords.get(lang, {})
    if not stopwords: return 1
    text = text.lower().strip()
    if is_cjk: 
      s_arr = list("".join(text.split())) 
    else: 
      s_arr = text.split()
    word_len = lang_2_max_stopword_len.get(lang, max_word_len)
    len_s = len(s_arr)
    stop_cnt = 0
    total_cnt = 0
    for i in range(len_s):
      if s_arr[i] is None: continue
      for j in range(min(len_s, i+word_len), i, -1):
        word = "".join(s_arr[i:j]) if is_cjk else " ".join(s_arr[i:j])
        if word in stopwords:
          stop_cnt += 1
          s_arr[i] = "".join(s_arr[i:j]) if is_cjk else " ".join(s_arr[i:j]) 
          for k in range(i+1, j):
            s_arr[k] = None
          break
      total_cnt += 1
    stopword_score =  (stop_cnt/total_cnt) 
    if is_cjk: stopword_score = stopword_score*cjk_scale
    return (stopword_score)


    
lang_2_max_flaggedword_len = dict([(lang, max(s.count(" ")+1 if not lang_is_cjk(lang) else len(s) for s in arr)) for lang, arr in flagged_words.items()])

def get_flaggedword_score(lang, text, max_word_len=3, cjk_scale=1.5):
    is_cjk = lang_is_cjk(lang)
    flaggedwords =  flagged_words.get(lang, set())
    en_flaggedwords = flagged_words["en"]
    flaggedwords = flaggedwords.union (en_flaggedwords)
    if not flaggedwords: return 0
    text = text.lower().strip()
    if is_cjk: 
      s_arr = list("".join(text.split())) 
    else: 
      s_arr = text.split()
    word_len = lang_2_max_flaggedword_len.get(lang, max_word_len)
    len_s = len(s_arr)
    flag_cnt = 0
    total_cnt = 0
    for i in range(len_s):
      if s_arr[i] is None: continue
      for j in range(min(len_s, i+word_len), i, -1):
        word = "".join(s_arr[i:j]) if is_cjk else " ".join(s_arr[i:j])
        if word in flaggedwords:
          flag_cnt += 1
          s_arr[i] = "".join(s_arr[i:j]) if is_cjk else " ".join(s_arr[i:j]) 
          for k in range(i+1, j):
            s_arr[k] = None
          break
      total_cnt += 1
    flaggedword_score =  (flag_cnt/total_cnt) 
    if is_cjk: flaggedword_score = flaggedword_score*cjk_scale
    return (flaggedword_score)


def get_score_moving_avg(lang, text, scores_per_lang=None, _stdev_lower_bound=2, _stdev_upper_bound=2, simple_moving_avg_window=500, fn=None, fn_args={}, use_mean_for_cutoff=True):
    if fn is None: fn = get_stopword_score
    if scores_per_lang is None: scores_per_lang = {}
    _max_cutoff = 10e9
    _min_cutoff = -10e9
    _stdev = _mean = _median = 0
    _score = fn(lang, text, **fn_args)
    _min_cutoff = _mean = _median = _stdev = _quantiles = None
    scores_per_lang[lang] = _scores = _scores_per_lang.get(lang,[])
    _scores.append(_score)
    if len(_scores) >= 2:
      if len(_scores) > simple_moving_avg_window:
         _scores_per_lang[lang] = _scores =_scores[-simple_moving_avg_window:]
      _stdev = statistics.stdev(_scores)
      _mean = statistics.mean (_scores)        
      _median = statistics.median (_scores) 
      if use_mean_for_cutoff:
        _min_cutoff = _mean-(_stdev*_stdev_lower_bound)
        _max_cutoff = _mean+(_stdev*_stdev_upper_bound)
      else:
        _min_cutoff = _median-(_stdev*_stdev_lower_bound)
        _max_cutoff = _median+(_stdev*_stdev_upper_bound)
    return _min_cutoff, _max_cutoff, _stdev, _mean, _median, scores_per_lang 



  
