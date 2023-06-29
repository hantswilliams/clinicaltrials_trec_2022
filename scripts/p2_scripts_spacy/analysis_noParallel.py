import math
import os
import json
import pandas as pd
import time
from tqdm import tqdm  


######## SIMPLIFIED STEPS ########
# 1 load in json file 
# 2 tokenize
# 3 calculate 
# 4 clear memory
# 1 load in new json file (repeat)
# .....perform 2-5 again (repeat)
########################


## capture start time 
starttime = time.time()

## load in json files
filelist = os.listdir('./s3_bucket/json/')
# filelist = filelist[:10000]

## create empty list for tokenized documents and idf dictionary
tokenized_documents = []
idf = {}

## initialize the progress bar with the total number of files
progress_bar = tqdm(filelist, desc="Processing files", unit="file")

## loop through each filelist item
for file in filelist:

    # update the progress bar
    progress_bar.set_postfix(file=file)
    
    ## 1 load in json file
    try:
        with open('./s3_bucket/json/' + file, 'r') as f:
            jsonData = json.load(f)
            doc = jsonData['textblock'][0]
    except:
        print("Error loading file: " + file)
        continue

    ## Step 2: Tokenize 
    all_terms = set()
    tokenized_doc = doc.split()  # Tokenize the document
    tokenized_documents.append(tokenized_doc)
    all_terms.update(tokenized_doc)

    ## Step 3: Calculate IDF 
    total_documents = len(tokenized_documents)
    for term in all_terms:
        doc_count = sum(1 for doc in tokenized_documents if term in doc)
        idf[term] = math.log(total_documents / (1 + doc_count))

    ## Step 4 Close JSON and remove assets from memory
    f.close()
    del jsonData, doc, all_terms, tokenized_doc, total_documents, term

    # update the progress bar
    progress_bar.set_postfix(file=file, completed="Done")
    progress_bar.update(1)

progress_bar.close() 

################################################################################################
################################################################################################

print ('done with all files, now creating dataframe...')
print ('\n')

## create dataframe from idf dictionary
idf_df = pd.DataFrame.from_dict(idf, orient='index', columns=['idf'])
idf_df = idf_df.sort_values(by=['idf'], ascending=False)
print(idf_df.head(10))
## save dataframe to csv

endtime = time.time()
print('total time: ', endtime - starttime)
