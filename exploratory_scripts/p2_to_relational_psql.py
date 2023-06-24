import json 
import os 

files = os.listdir('temp')

keys = []

for i in range(len(files)):
    file_name = files[i]
    with open(f"temp/{files[i]}", "r") as f:
        json_content = json.load(f)
    keys.append(list(json_content.keys()))

keys = [item for sublist in keys for item in sublist]
len(keys)

## remove duplicates
keys = list(set(keys))
len(keys)

## figure out the names of the columns to create 
columnnames = keys

## sort column names alphabetically
columnnames.sort()