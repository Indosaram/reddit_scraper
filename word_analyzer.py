import os
import json

import nltk
import pandas as pd


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
        frequencies = freq.most_common()

        return frequencies

    def _load_csv(self, filename):
        print(f"Loading file: {filename}")
        df = pd.read_csv(filename, dtype=str)
        text = []
        for value in df[df["subreddit"] == "deeplearning"]["selftext"]:
            text.append(value)
        text = " ".join(text)
        return text

    def get_frequency(self, filename, num_words=3):
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


if __name__ == '__main__':
    word_analyzer = WordAnalyzer()
    word_analyzer.get_frequency("data/RS_2020-06.csv")
