import re
    
def cjk_detect(texts):
    # chinese
    if re.search("[\u4e00-\u9FFF]", texts):
        return "zh"
    # korean
    if re.search("[\uac00-\ud7a3]", texts):
        return "ko"
    # japanese
    if re.search("[\u3040-\u30ff]", texts):
        return "ja"
    # thai
    if re.search("[\u0E01-\u0E5B]", texts):
        return "th"
    # traditional javanese
    if re.search("[\uA980-\uA9DF]", texts):
       return "jv_tr"
    return None
    
def lang_is_cjk(lang):
    return lang in {'zh', 'zh-classical', 'zh-min-nan', 'zh-yue', 'ko', 'ja', 'th', 'jv_tr'}
