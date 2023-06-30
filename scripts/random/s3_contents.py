import awswrangler as wr
import json

fileList_s3 = wr.s3.list_objects('s3://clinicaltrials-gov/') ## note, this might take about 60 seconds to run
fileList_s3 = [file for file in fileList_s3 if file.endswith('.json')]

## save the file list to a json file
with open('./data/fileList_s3.json', 'w') as f:
    json.dump(fileList_s3, f)
f.close()