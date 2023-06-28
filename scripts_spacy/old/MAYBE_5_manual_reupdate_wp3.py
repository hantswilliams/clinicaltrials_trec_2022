import math
import os
import json
import pandas as pd
import time
from tqdm import tqdm
from concurrent import futures
import re
import medspacy

######## SIMPLIFIED STEPS ########
# 1 load in json file
# 2 tokenize
# 3 store term frequency
# 4 clear memory
# 1 load in new json file (repeat)
# .....perform 2-4 again (repeat)
# calculate IDF for each term after all documents have been processed
########################

### Helper Functions ###
def json_cleaning(text):
    pattern = r"[^\w\s]"
    clean_text = re.sub(pattern, '', text)    
    clean_text = clean_text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
    clean_text = clean_text.lower()
    clean_text = ' '.join(clean_text.split())
    return clean_text

nlp = medspacy.load("en_core_sci_sm")

## capture start time
starttime = time.time()

## load in json files
filelist = os.listdir('./s3_bucket/json/')
filelist = filelist[:5000]

## define the function to process a single file
def process_file(file):
    try:
        with open('./s3_bucket/json/' + file, 'r') as f:
            jsonData = json.load(f)
            doc = json_cleaning(jsonData['textblock'][0])
    except:
        print("Error loading file: " + file)
        return None  # Return None if error occurs
    
    ## Step 2: Tokenize and clean
    tokenized_doc = doc.split()  # Tokenize the document
    tokenized_doc = [word for word in tokenized_doc if word not in nlp.Defaults.stop_words]
    
    ## Step 3: Calculate term frequency
    term_set = set(tokenized_doc)

    return term_set

## create empty list for term frequency dictionaries
term_sets = []

## initialize the progress bar with the total number of files
progress_bar = tqdm(filelist, desc="Processing files", unit="file")

## create a ProcessPoolExecutor with the number of desired processors
with futures.ProcessPoolExecutor() as executor:
    ## loop through each filelist item and submit the tasks to the executor
    future_results = [executor.submit(process_file, file) for file in filelist]

    ## wait for all futures to complete
    for future in futures.as_completed(future_results):
        term_set = future.result()

        if term_set is not None:
            term_sets.append(term_set)

        # update the progress bar
        progress_bar.set_postfix(completed=f"{progress_bar.n}/{progress_bar.total}")
        progress_bar.update(1)

progress_bar.close()

## Step 5: Calculate IDF for each term after all documents have been processed
idf = {}
for term_set in term_sets:
    for term in term_set:
        if term not in idf:
            idf[term] = sum(1 for doc_set in term_sets if term in doc_set)

idf = {term: math.log((len(filelist) + 1) / (count + 1)) for term, count in idf.items()}

## Print the IDF values
for term, value in idf.items():
    print(term, value)

## capture end time
endtime = time.time()
print("Execution Time:", endtime - starttime, "seconds")

print('done with all files, now creating dataframe...')
print('\n')
