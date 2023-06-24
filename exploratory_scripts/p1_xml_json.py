import pandas as pd
import json
import os 

# # get list of files that exist in /data/sample
files = os.listdir('data/sample')
total_count = len(files)

for i in range(len(files)):
    ## print file name
    print('working on file: ', files[i])

    ## file name without .xml 
    file_name = files[i].split('.')[0]

    ## load in a xml file
    df = pd.read_xml(f'data/sample/{files[i]}')

    ## create some study object
    studyobject = []

    ## loop through each column in df, getting the values, dropping the NaNs, and appending to studyobject
    for col in df.columns:
        col_values = df[col].dropna().values
        col_name = col 
        values = {col: col_values}
        studyobject.append(values)

    ## flatten the studyobject // nice simple dictionary 
    studyobject_flat = {k: v for d in studyobject for k, v in d.items()}

    ## save json to file
    with open(f'temp/{file_name}.json', 'w') as f:
        json.dump(studyobject_flat, default=lambda x: x.tolist(), indent=4, fp=f)

    ## print completion number remaining
    print('completed: ', files[i], ' | ', i, ' of ', total_count)


### note - decided to not take this approach, manually force evey value to be part of a list/array, versus using xmltodict
### in which sometimes things will be strings (single value) versus multiple values (list/array) - best just to force everything to be a list/array
## for simlicity sake for now
# import xml.etree.ElementTree as ET
# import numpy as np
# import xmltodict
# files = os.listdir('data/sample')
# for i in range(len(files)):
#     file_name = files[i]
#     with open(f"data/sample/{files[i]}", "r") as f:
#         xml_content = f.read()
#     data_dict = xmltodict.parse(xml_content)
#     ## save json to file
#     with open(f'temp/{file_name}.json', 'w') as f:
#         json.dump(data_dict, indent=4, fp=f)
#     print('completed: ', files[i])
    
