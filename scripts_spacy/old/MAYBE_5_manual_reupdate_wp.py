import math
import os
import json
import pandas as pd
import time
import multiprocessing
from functools import partial


def process_file(file, tokenized_documents, idf, progress):
    ## 1 load in json file
    try:
        with open('./s3_bucket/json/' + file, 'r') as f:
            jsonData = json.load(f)
            doc = jsonData['textblock'][0]
    except:
        print("Error loading file: " + file)
        progress[file] = 'Error'
        return None

    # update the progress
    progress[file] = 'Tokenizing'

    ## Step 2: Tokenize
    tokenized_doc = doc.split()  # Tokenize the document
    tokenized_documents.append(tokenized_doc)

    # update the progress
    progress[file] = 'Calculating IDF'

    ## Step 3: Calculate IDF
    total_documents = len(tokenized_documents)
    all_terms = set(tokenized_doc)
    for term in all_terms:
        doc_count = sum(1 for doc in tokenized_documents if term in doc)
        idf[term] = math.log(total_documents / (1 + doc_count))

    ## Step 4: Close JSON and remove assets from memory
    f.close()
    del jsonData, doc, tokenized_doc

    # update the progress
    progress[file] = 'Done'


def process_files():
    ## capture start time
    starttime = time.time()

    ## load in json files
    filelist = os.listdir('./s3_bucket/json/')

    ## create shared list for tokenized documents and shared dictionary for idf
    manager = multiprocessing.Manager()
    tokenized_documents = manager.list()
    idf = manager.dict()

    ## create shared array for progress
    progress = multiprocessing.Array('c', len(filelist))

    # multiprocessing
    pool = multiprocessing.Pool(processes=multiprocessing.cpu_count())

    # partial function
    process_file_partial = partial(
        process_file, tokenized_documents=tokenized_documents, idf=idf, progress=progress
    )

    # execute function
    pool.map(process_file_partial, filelist)
    pool.close()
    pool.join()

    # print progress status
    for idx, status in enumerate(progress):
        file = filelist[idx]
        print(f"File: {file}, Status: {status.decode()}, Files Remaining: {len(filelist) - idx - 1}")

    ## calculate elapsed time
    elapsedtime = time.time() - starttime
    print("Elapsed Time: ", elapsedtime)

    ## Files Processed
    print("Total Files Processed: ", len(filelist))

    ## create dataframe from idf dictionary
    idf_df = pd.DataFrame.from_dict(idf, orient='index', columns=['idf'])
    idf_df = idf_df.sort_values(by=['idf'], ascending=False)
    print(idf_df.head(10))


if __name__ == "__main__":
    process_files()
