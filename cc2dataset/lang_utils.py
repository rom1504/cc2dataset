import spacy_fastlang
import spacy

class LangDetection:
    def __init__(self) -> None:
        self.model = spacy.load("en_core_web_sm")
        self.model.add_pipe("language_detector")

    def detect(self, text: str) -> str:
        doc = self.model(text)
        return doc._.language