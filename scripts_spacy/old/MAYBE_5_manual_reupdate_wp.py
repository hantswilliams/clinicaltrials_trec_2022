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

    ##
