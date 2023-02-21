import fasttext
from pathlib import Path

def load_fasttext_model(path_fasttext_model):
    print("Loading fasttext model from ", path_fasttext_model)
    return fasttext.load_model(path_fasttext_model)

def get_fasttext_info(line, model_lang_id):
    """The line should be in lower case and without \n in it."""
    pred = model_lang_id.predict(line)
    lang_pred_fasttext_id = pred[0][0].replace("__label__", "")
    score_pred = pred[1][0]
    return lang_pred_fasttext_id, score_pred



class LangDetection:
    #adapted from https://github.com/bigcode-project/bigcode-analysis/blob/main/data_analysis/python_data_analysis/nl_language_identification/language_identifier.py
    def __init__(self,model_dump_path:str) -> None:
        self.lang_model_path : str = model_dump_path
        self.model = load_fasttext_model(self.lang_model_path)


    def detect(self, text: str) -> str:
        """
        Detects the language of the text
        args:
            text (str) : Text to detect the language

        returns:
            language (str) : Predicted Language of the text
            score_pred (str) : confidence of the prediction

        """
        text = text.lower()

        fasttext_pred = get_fasttext_info(
            text, self.model
        )
        return fasttext_pred[0], fasttext_pred[1]


import re


class LicensePattern:
    cc_pattern = re.compile("http[s]?://creativecommons\\.org/licenses/(by|by-sa|by-nd|by-nc|by-nc-sa|by-nc-nd|publicdomain)[\"/ >]")

def detect_licence(html:str):
    """
    Given a HTML string, this function detects the licence of the page.
    It returns a string with the licence name, or NO-LICENCE-FOUND if no licence is found.
    """
    license_attribute_pattern = re.compile(LicensePattern.cc_pattern)

    # storing counts of all difference occurrences of link to CC
    multiple_occurrences_map = {}

    # add all of them to the list
    for match in license_attribute_pattern.finditer(html):
        licence = match.group(1)

        # add entry
        if licence not in multiple_occurrences_map:
            multiple_occurrences_map[licence] = 0

        # and increase count
        multiple_occurrences_map[licence] += 1

    # no licence found
    if not multiple_occurrences_map:
        return "no-licence-found"

    # only one link found or if multiple links found but the same type
    if len(multiple_occurrences_map) == 1:
        return list(multiple_occurrences_map.keys())[0]

    # if multiple different links found, we return a general CC-UNSPECIFIED
    return "cc-unspecified"


if __name__ == "__main__":
    lang_detector = LangDetection("lid_model_dump/lid.176.bin")
    print(lang_detector.detect("Das ist ein Test."))

#output : ('de', 1.000038981437683)