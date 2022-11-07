import math
import spacy
nlp = spacy.load('en_core_web_sm')

from fasttext import load_model
FASTTEXT_LANG_MODEL = "fasttext-lang.ftz"
FASTTEXT_LANG_MODEL = load_model(FASTTEXT_LANG_MODEL)

def get_sentences(text : str) -> list:
    '''Get sentences from text'''
    nlp_text = nlp(text)
    sentences = list(nlp_text.sents)
    
    # Preprocess
    sentences = [sen.text.replace("\n", " ") for sen in sentences]
    sentences = [sen for sen in sentences if len(sen.split(" ")) > 3]

    return sentences

def predict_lang(text : str) -> list:
    '''
    Predict the language of a given text and return label of of predicted language.
    Returns in this way: [{"lang" : "en", "x" : 0.99}, ...]
    '''

    # predict returns something like(("label_1", "label_2"), array(0.4, 0.5))
    labels, scores = FASTTEXT_LANG_MODEL.predict(text, k=3)

    # Get only langs that could be (score above 0.25)
    predictions = []
    for label, score in zip(labels, scores):
        if score > 0.25:
            predictions.append((label.replace("__label__", ""), score))


    return [{"lang" : label, "x" : score} for label, score in predictions]

class TextLangs:
    def __init__(self, text : str):
        self.sentences = get_sentences(text)
        self.langs = {} # lang : {"x" : 0.5, sent_idx : [0,5], word_count : 10}
        self.detect()

    def detect(self) -> None:
        '''Detect langs for all sentences'''
        for sent_idx, sen in enumerate(self.sentences):
            # Get sentence len
            word_count = len(sen.split(" "))
            
            # Handle all predicted langs
            for pred in predict_lang(sen):
                # Multiply score by sqrt(sentence len)
                pred["x"] = pred["x"] * math.sqrt(word_count)

                # Add it to the lang dict
                if pred["lang"] not in self.langs:
                    self.langs[pred["lang"]] = {"x" : 0, "sent_idx" : [], "word_count" : 0}              
                self.langs[pred["lang"]]["sent_idx"] += [sent_idx]
                self.langs[pred["lang"]]["x"] += pred["x"]
                self.langs[pred["lang"]]["word_count"] += word_count

        # Calc percentage for each lang
        total_score = sum([lang["x"] for lang in self.langs.values()])
        for name, item in self.langs.items():
            self.langs[name]["x"] = item["x"] / total_score

    def get_detected_text(self, filter : str = None) -> list:
        '''Get detected text. Output format: [text, {"en" : 0.3, "fr" : 0.7}]'''
        text = self.sentences
        for idx, sent in enumerate(text):
            langs = []
            for lang, score in self.langs.items():
                if idx in score["sent_idx"]:
                    langs += [lang]
            
            if filter is None: # No filter
                text[idx] = [sent, langs]
            elif filter in langs: 
                # Filter in lang
                text[idx] = sent
            else:
                # Sent not in filtered for lang
                text[idx] = ""

        return text

    def get_detected_langs(self) -> list:
        '''Get detected langs. Output format: [{"lang" : "en" ,"x" : 0.3}]'''
        langs = []
        for lang, item in self.langs.items():
            if item["x"] >= 0.25:
                langs.append({"lang" : lang, "x" : item["x"], "word_count" : item["word_count"]})

        return langs


def get_post_text(post : dict) -> str:
    '''Get post text'''
    text = ""
    if "text" in post:
        text += post["text"] + ". "
    if "text_title" in post:
        text += post["text_title"] + ". "
    if "text_body" in post:
        text += post["text_body"] + ". "

    return text

