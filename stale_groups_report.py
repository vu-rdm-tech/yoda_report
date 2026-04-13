import json
import pandas as pd
from datetime import datetime, timedelta
import os, glob

def find_newest_json(directory):
    list_of_files = glob.glob(f'{directory}/*')  # get list of all json files
    if not list_of_files:  # if list is empty, return None
        return None
    latest_file = max(list_of_files, key=os.path.getctime)  # find the latest file
    return latest_file


# groups were newest file is older than <days> ago. Remember irods timestamps are always the date the file was updated on irods, not the original file date.
days=183
cutoff = datetime.now() - timedelta(days=days)


latest_file = find_newest_json('data/archived')
print(latest_file)
with open(latest_file, 'r') as fp:
    data = json.load(fp)

today = datetime.now()
reportfile = f'data/stalegroups/stale-groups_{latest_file[24:30]}_{days}.csv'
print(reportfile)
with open(reportfile, "w") as f:
    list={'group': [], 'newest':[], 'size': []}
    for collection in data['collections']:
        if collection.startswith('research-'):
            size =  data["collections"][collection]["size"]
            newest = data["collections"][collection]["newest"]
            print(newest)
            if datetime.strptime(newest[:19], "%Y-%m-%dT%H:%M:%S") < cutoff and not newest == "1970-01-01T00:00:00":
                list['group'].append(collection)
                list['newest'].append(newest)
                list['size'].append(size)
                f.write(f"{collection},{newest},{size}\n")
                print(collection, newest, size)
