"""
Archived data can be downloaded from http://files.pushshift.io/reddit/submissions/
"""

import re
import os
from functools import partial
from multiprocessing import Pool, cpu_count
import subprocess
import json

import pandas as pd


class ArchivedReddit:
    def __init__(self, data_dir="data"):
        self.data_dir = os.path.join(os.getcwd(), data_dir)

    @staticmethod
    def _process_json(filename):
        """
        archived json file has each line of json data, therefore first parse by
        line and load a line to dictionrary."""

        print(f"Start processing {filename}")
        temp_dict = {'subreddit': [], 'title': [], 'selftext': [], 'url': []}

        with open(filename, "r") as file:
            for line in file:
                data = json.loads(line)

                if not data["selftext"] or data["selftext"] in [
                    "[deleted]",
                    "[removed]",
                ]:
                    continue

                for key in temp_dict.keys():
                    temp_dict[key].append(data.get(key, ""))
            print(f"Converting {filename} to dataframe")
            df = pd.DataFrame(temp_dict)
            print(f"Saving {filename} to csv file")
            df.to_csv(f"{filename}.csv")
        print(f"Finish processing {filename}")

    def process_files(self):
        filenames = [
            os.path.join(self.data_dir, filename)
            for filename in os.listdir(self.data_dir)
            if not filename.endswith('.csv')
        ]

        for file_name in filenames:
            self._process_json(file_name)

    def download_file(self, url):
        file_name = re.search("(?:.*\/)+(.*\..*)", url).group(1)
        self.execute(
            f"wget -4 -O {os.path.join(self.data_dir,file_name)} {url}"
        )

    @staticmethod
    def execute(command, shell=True):
        print('✔', command)
        try:
            process = subprocess.run(
                command, shell=shell, check=True, capture_output=True
            )
        except subprocess.CalledProcessError as e:
            raise e

        try:
            result = process.stdout.decode('cp949')
        except UnicodeDecodeError:
            result = None

        return result

    def _decompress(self, filename):
        # Overwite existing file, remove source file
        command = (
            "zstd -f --rm --memory=2048MB  --decompress"
            f" {os.path.join(self.data_dir,filename)}"
        )
        self.execute(command)

    def decompress(self):
        filenames = [
            os.path.join(self.data_dir, filename)
            for filename in os.listdir(self.data_dir)
            if filename.endswith('.zst')
        ]

        for file_name in filenames:
            self._decompress(file_name)

if __name__ == "__main__":
    archived_reddit = ArchivedReddit()

    min_month = "2020-04"
    max_month = "2020-05"

    dates = pd.period_range(min_month, max_month, freq='M')

    uris = [
        f"http://files.pushshift.io/reddit/submissions/RS_{date}.zst"
        for date in dates
    ]

    num_cpu = cpu_count()
    print("There are {} CPUs on this machine ".format(num_cpu))

    print("Start downloading files")
    pool = Pool(min(len(uris), num_cpu))
    pool.map(partial(archived_reddit.download_file), uris)
    pool.close()
    pool.join()

    archived_reddit.decompress()
    archived_reddit.process_files()
