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
# 3 calculate
# 4 clear memory
# 1 load in new json file (repeat)
# .....perform 2-5 again (repeat)
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
filelist = filelist[:1000]

## define the function to process a single file
def process_file(file):

    tokenized_doc = []  # Initialize tokenized_doc list
    idf_result = {}  # Initialize idf_result dictionary
    
    try:
        with open('./s3_bucket/json/' + file, 'r') as f:
            jsonData = json.load(f)
            doc = json_cleaning(jsonData['textblock'][0])
    except:
        print("Error loading file: " + file)
        return tokenized_doc, idf_result  # Return empty values
    
    ## Step 2: Tokenize and clean
    tokenized_doc = doc.split()  # Tokenize the document
    tokenized_doc = [word for word in tokenized_doc if word not in nlp.Defaults.stop_words]

    ## Step 3: Calculate IDF
    all_terms = set(tokenized_doc)
    doc_count = {term: sum(1 for doc in tokenized_documents if term in doc) + (term in tokenized_doc) for term in all_terms}
    idf_result = {term: math.log((len(filelist) + 1) / (count + 1)) for term, count in doc_count.items()}

    ## Step 4: Close JSON and remove assets from memory
    f.close()
    del jsonData, doc, all_terms

    return tokenized_doc, idf_result

## create empty list for tokenized documents and idf dictionary
tokenized_documents = []
idf = {}

## initialize the progress bar with the total number of files
progress_bar = tqdm(filelist, desc="Processing files", unit="file")

## create a ProcessPoolExecutor with the number of desired processors
with futures.ProcessPoolExecutor() as executor:
    ## loop through each filelist item and submit the tasks to the executor
    future_results = [executor.submit(process_file, file) for file in filelist]

    ## wait for all futures to complete
    for future in futures.as_completed(future_results):
        tokenized_doc, idf_result = future.result()

        if tokenized_doc is not None:
            tokenized_documents.append(tokenized_doc)

        if idf_result is not None:
            for term, value in idf_result.items():
                if term in idf:
                    idf[term] += value
                else:
                    idf[term] = value

        # update the progress bar
        progress_bar.set_postfix(completed=f"{progress_bar.n}/{progress_bar.total}")
        progress_bar.update(1)

progress_bar.close()

## Get the unique terms from all documents
all_terms = set()
for doc in tokenized_documents:
    all_terms.update(doc)

## Calculate the number of documents that contain each term
print("Calculating document frequency...")
doc_count = {term: sum(1 for doc in tokenized_documents if term in doc) for term in all_terms}

## Calculate the IDF for each term across all documents
print("Calculating IDF...")
idf = {term: math.log((len(filelist) + 1) / (count + 1)) for term, count in doc_count.items()}

## Print the IDF values
for term, value in idf.items():
    print(term, value)

## capture end time
endtime = time.time()
print("Execution Time:", endtime - starttime, "seconds")



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
print('\n')
print(idf_df.tail(10))
## save dataframe to csv

endtime = time.time()
print('total time: ', endtime - starttime)
