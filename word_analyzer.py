import json
import os
import ssl

import nltk
import pandas as pd

try:
    _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:
    pass
else:
    ssl._create_default_https_context = _create_unverified_https_context


class WordAnalyzer:
    def __init__(self, result_dir="result"):
        datum = ['punkt', 'averaged_perceptron_tagger']
        for data in datum:
            nltk.download(data)

        self.result_dir = result_dir

    def _extract(self, text, num_words):
        print("Extracting frequent words")
        tokens = nltk.word_tokenize(text.lower())
        tagged_tokens = nltk.pos_tag(tokens)

        words = [
            word
            for word, pos in tagged_tokens
            if pos in ('VERBS', 'NN', 'NNP', 'NNS', 'NNPS') and len(word) >= 2
        ]

        freq = nltk.FreqDist(words)
        if num_words is not None:
            frequencies = freq.most_common(num_words)
        else:
            frequencies = freq.most_common(num_words)

        return frequencies

    def _load_csv(self, filename):
        print(f"Loading file: {filename}")
        df = pd.read_csv(filename, dtype=str)
        text = []
        for value in df[df["subreddit"] == "deeplearning"]["selftext"]:
            text.append(value)
        text = " ".join(text)
        return text

    def get_frequency(self, filename, num_words=None):
        text = self._load_csv(filename)
        frequencies = self._extract(text, num_words)

        result = {}
        for word, freq in frequencies:
            result[word] = freq

        with open(
            os.path.join(
                self.result_dir, filename.split("/")[1].split(".")[0] + ".json"
            ),
            "w",
        ) as file:
            json.dump(result, file, indent=4)

    def merge_months(self, filenames):
        for file in os.listdir(self.result_dir):
            ...
        # TODO: multiple month trends


if __name__ == '__main__':
    word_analyzer = WordAnalyzer()

    files = ["RS_2005-06.csv"]
    for file in files:
        word_analyzer.get_frequency(f"data/{file}")
