import pandas as pd
import json
import io
import os
from exploratory_scripts.p2_files import get_files_paths
import boto3
import dotenv
import datetime

dotenv.load_dotenv()

# ##### AWS S3 #####
# session = boto3.Session(
#     profile_name='nhit', 
#     region_name='us-east-1',
#     aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
#     aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
# )
# s3 = session.resource('s3')

##### GET FILES 
files = get_files_paths()
# files = files[0:100]
len(files)
total_count = len(files)

errors = []

starttime = datetime.datetime.now().strftime("%H:%M:%S")
for i in range(len(files)):
    print('working on file: ', files[i])

    ## just get the filename without the path
    file_name = files[i].split('/')[-1].split('.')[0]

    ## load in a xml file
    df = pd.read_xml(f'{files[i]}')

    ## create some study object
    studyobject = []

    ## loop through each column in df, getting the values, 
    ## dropping the NaNs, and appending to studyobject
    for col in df.columns:
        col_values = df[col].dropna().values
        col_name = col 
        values = {col: col_values}
        studyobject.append(values)

    ## flatten the studyobject // nice simple dictionary 
    studyobject_flat = {k: v for d in studyobject for k, v in d.items()}

    ## save file locally
    with open(f'temp/{file_name}.json', 'w') as f:
        json.dump(studyobject_flat, default=lambda x: x.tolist(), fp=f)

    # ## using io create a temp file in memory
    # temp = io.StringIO()

    # ## dump json to temp file
    # json.dump(studyobject_flat, default=lambda x: x.tolist(), fp=temp)

    ## print temp file
    # print(temp.getvalue())

    # try:
    #     s3.Object('clinicaltrials-gov', f'{file_name}.json').put(Body=temp.getvalue())
    # except Exception as e:
    #     print(e)
    #     errors.append(files[i])
    #     pass

    ## print completion number remaining
    # print('completed: ', files[i], ' | ', i, ' of ', total_count)

    ## print number of files remaining
    print('files remaining: ', total_count - i)
endtime = datetime.datetime.now().strftime("%H:%M:%S")
total_time_minutes = (datetime.datetime.strptime(endtime, "%H:%M:%S") - datetime.datetime.strptime(starttime, "%H:%M:%S")).total_seconds() / 60




















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
    
