import math
import os
import json
import pandas as pd
import time
from tqdm import tqdm
from concurrent import futures

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
filelist = filelist[:10000]

## create empty list for tokenized documents and idf dictionary
tokenized_documents = []
idf = {}

## define the function to process a single file
def process_file(file):
    try:
        with open('./s3_bucket/json/' + file, 'r') as f:
            jsonData = json.load(f)
            doc = jsonData['textblock'][0]
    except:
        print("Error loading file: " + file)
        return None
    
    ## Step 2: Tokenize
    tokenized_doc = doc.split()  # Tokenize the document

    ## Step 3: Calculate IDF
    all_terms = set(tokenized_doc)
    doc_count = {term: sum(1 for doc in tokenized_documents if term in doc) for term in all_terms}
    idf = {term: math.log(len(tokenized_documents) / (1 + count)) if count > 0 else 0 for term, count in doc_count.items()}

    ## Step 4: Close JSON and remove assets from memory
    f.close()
    del jsonData, doc, all_terms, tokenized_doc

    return idf

## initialize the progress bar with the total number of files
progress_bar = tqdm(filelist, desc="Processing files", unit="file")

## create a ProcessPoolExecutor with the number of desired processors
with futures.ProcessPoolExecutor() as executor:
    ## loop through each filelist item and submit the tasks to the executor
    futures = [executor.submit(process_file, file) for file in filelist]

    ## wait for all futures to complete
    for future in futures:
        idf_result = future.result()

        if idf_result is not None:
            tokenized_documents.append(idf_result)

        # update the progress bar
        progress_bar.set_postfix(completed=f"{progress_bar.n}/{progress_bar.total}")
        progress_bar.update(1)

progress_bar.close()

################################################################################################
################################################################################################

print('done with all files, now creating dataframe...')
print('\n')

print('tokenized_documents: ', tokenized_documents)
print('Length of tokenized_documents: ', len(tokenized_documents))
print('\n')
print('idf: ', idf)
print('Length of idf: ', len(idf))

## create dataframe from idf dictionary
idf_df = pd.DataFrame.from_dict(idf, orient='index', columns=['idf'])
idf_df = idf_df.sort_values(by=['idf'], ascending=False)
print(idf_df.head(10))
## save dataframe to csv

endtime = time.time()
print('total time: ', endtime - starttime)
