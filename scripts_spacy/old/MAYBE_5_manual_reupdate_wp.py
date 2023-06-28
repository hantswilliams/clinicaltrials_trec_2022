import math
import os
import json
import pandas as pd
import time
import multiprocessing
from functools import partial

######## SIMPLIFIED STEPS ########
# 1 load in json file
# 2 tokenize
# 3 calculate
# 4 clear memory
# 1 load in new json file (repeat)
# .....perform 2-5 again (repeat)
########################

def process_file(file, tokenized_documents, idf, progress):
    # update the progress
    progress[file] = 'Processing'

    ## 1 load in json file
    try:
        with open('./s3_bucket/json/' + file, 'r') as f:
            jsonData = json.load(f)
            doc = jsonData['textblock'][0]
    except:
        print("Error loading file: " + file)
        progress[file] = 'Error'
        return None

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

    # update the progress
    progress[file] = 'Done'


def process_files():
    ## capture start time
    starttime = time.time()

    ## load in json files
    filelist = os.listdir('./s3_bucket/json/')

    ## create empty list for tokenized documents and idf dictionary
    tokenized_documents = []
    idf = {}

    ## initialize the progress dictionary
    progress = multiprocessing.Manager().dict()

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
    for file, status in progress.items():
        print(f"File: {file}, Status: {status}, Files Remaining: {len(filelist) - len(progress)}")

    ## calculate elapsed time
    elapsedtime = time.time() - starttime
    print("Elapsed Time: ", elapsedtime)

    ## Files Processed
    print("Total Files Processed: ", len(filelist))

    print ('done with all files, now creating dataframe...')
    print ('\n')

    print(idf)

    # ## create dataframe from idf dictionary
    # idf_df = pd.DataFrame.from_dict(idf, orient='index', columns=['idf'])
    # idf_df = idf_df.sort_values(by=['idf'], ascending=False)
    # print(idf_df.head(10))



################################################################################################
################################################################################################


if __name__ == "__main__":
    process_files()





