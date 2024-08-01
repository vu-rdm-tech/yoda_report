import json
import pandas as pd
from datetime import datetime
import os, glob

def friendly_size(num):
    """
    this function will convert bytes to MB.... GB... etc
    """
    for x in ['bytes', 'KB', 'MB', 'GB', 'TB']:
        if num < 1024.0:
            return "%3.1f %s" % (num, x)
        num /= 1024.0

def find_newest_json(directory):
    list_of_files = glob.glob(f'{directory}/*')  # get list of all json files
    if not list_of_files:  # if list is empty, return None
        return None
    latest_file = max(list_of_files, key=os.path.getctime)  # find the latest file
    return latest_file


#latest_file = find_newest_json('data/archived')
latest_file = find_newest_json('data')
print(latest_file)
with open(latest_file, 'r') as fp:
    data = json.load(fp)
    names=[]
    for col in data['collections']:
        if col.startswith('vault-'):
            if len(data['collections'][col]['datasets']) == 1:
                names.append(col[6:])
    total = 0
    for name in names:
        try:
            research_size = data['collections'][f'research-{name}']['size']
            research_count = data['collections'][f'research-{name}']['count']
            dsname=list(data['collections'][f'vault-{name}']['datasets'])[0]
            dataset_size = data['collections'][f'vault-{name}']['datasets'][dsname]['original_size']
            dataset_count = data['collections'][f'vault-{name}']['datasets'][dsname]['original_count']
            if research_size==dataset_size and research_count==dataset_count:
                total = total + dataset_size
                print(f'research-{name}', friendly_size(dataset_size))
        except:
            pass
print(f'To be deleted: {friendly_size(total)}')
