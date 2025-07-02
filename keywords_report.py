import json
import pandas as pd
from datetime import datetime
import os, glob

def find_newest_json(directory):
    list_of_files = glob.glob(f'{directory}/*')  # get list of all json files
    if not list_of_files:  # if list is empty, return None
        return None
    latest_file = max(list_of_files, key=os.path.getctime)  # find the latest file
    return latest_file


latest_file = find_newest_json('data/archived')
print(latest_file)
with open(latest_file, 'r') as fp:
    data = json.load(fp)
    for collection in data['collections']:
        if 'datasets' in data['collections'][collection]:
            for dataset in data['collections'][collection]["datasets"]:
                if data['collections'][collection]["datasets"][dataset]["status"]=="PUBLISHED":
                    for keyword in data['collections'][collection]["datasets"][dataset]["keywords"]:
                        if ',' in keyword or ';' in keyword:
                            # https://portal.yoda.vu.nl/vault/?dir=%2Fvault-beta-cps-tcda%2FBackup%20VU%20Bazis%202023-2024%5B1722506503%5D
                            print(f'"https://portal.yoda.vu.nl/vault/?dir=%2F{collection}%2F{dataset.replace(" ", "%20")}"\t"{keyword}"')