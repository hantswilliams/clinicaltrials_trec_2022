import math
from collections import Counter
import time
import pandas as pd
import medspacy
import spacy

import os
import json
import re
from multiprocessing import Pool, Manager

nlp = medspacy.load("en_core_sci_sm")

def json_cleaning(text):
    pattern = r"[^\w\s]"
    clean_text = re.sub(pattern, '', text)    
    clean_text = clean_text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
    clean_text = ' '.join(clean_text.split())
    return clean_text

def calculate_idf(term, docs, total_docs):
    doc_count = sum(1 for doc in docs if term in doc)
    return term, math.log(total_docs / (1 + doc_count))

def load_and_clean_file(file_path):
    try:
        with open(file_path, 'r') as f:
            jsonData = json.load(f)
            jsonData['textblock'][0] = json_cleaning(jsonData['textblock'][0])
            return jsonData['textblock'][0]
    except:
        print("Error loading file: " + file_path)
        return None

def process_document(doc, processed_documents):
    doc = doc.split()
    doc = [word for word in doc if word not in nlp.Defaults.stop_words]
    processed_documents.append(doc)

def process_files():
    ##### Data files 
    directory = "./s3_bucket/json/"
    fileNames = os.listdir(directory)

    ## load first X files from fileNames into a list of strings, just the textblock
    documents = []
    filesNotLoaded = []
    fileCount = 400000
    for i in range(fileCount):
        file_path = os.path.join(directory, fileNames[i])
        document = load_and_clean_file(file_path)
        if document is not None:
            documents.append(document)
        else:
            filesNotLoaded.append(fileNames[i])

    newFileCount = len(documents)
    newFileNames = fileNames[0:newFileCount]

    manager = Manager()
    processed_documents = manager.list()
    for doc in documents:
        process_document(doc, processed_documents)
    tokenized_documents = list(processed_documents)

    # Calculate the term frequency for each document
    term_frequency = [Counter(doc) for doc in tokenized_documents]

    # Combine all the documents into one list
    all_terms = set()
    for doc in tokenized_documents:
        all_terms.update(doc)

    total_documents = len(documents)
    starttime = time.time()

    with Pool(processes=5) as pool:
        idf_results = pool.starmap(calculate_idf, [(term, tokenized_documents, total_documents) for term in all_terms])
        idf = dict(idf_results)

    endtime = time.time()
    print("Time to calculate IDF: " + str(endtime - starttime))

    # Calculate the TF-IDF for each term in each document
    tf_idf = []  # CONTAINS ALL THE TF-IDF SCORES
    for doc_tf in term_frequency:
        doc_tfidf = {}
        total_terms = sum(doc_tf.values())
        for term, freq in doc_tf.items():
            doc_tfidf[term] = freq / total_terms * idf[term]
        tf_idf.append(doc_tfidf)

    # Limit the number of terms per document
    max_terms_per_document = 150  # CHANGE THIS TO LIMIT THE NUMBER OF TERMS PER DOCUMENT 150
    limited_tf_idf = []  # CONTAINS ALL THE TF-IDF SCORES, LIMITED TO max_terms_per_document
    for doc_tfidf in tf_idf:
        sorted_terms = sorted
        sorted_terms = sorted(doc_tfidf.items(), key=lambda x: x[1], reverse=True)[:max_terms_per_document]
        limited_tf_idf.append(dict(sorted_terms))

    # Get the overall term weights for each term
    term_weights = {}
    for doc_tfidf in limited_tf_idf:
        for term, score in doc_tfidf.items():
            if term not in term_weights or score > term_weights[term]:
                term_weights[term] = score

    # Print the term weights, sorted by weight
    termweights_df = pd.DataFrame.from_dict(term_weights, orient='index', columns=['weight'])
    termweights_df = termweights_df.sort_values(by=['weight'], ascending=False)

    # Create a dictionary of the term weights for each document, where the index is the document filename
    doc_weights = {}
    for i, doc_tfidf in enumerate(limited_tf_idf):
        doc_weights[newFileNames[i]] = {}
        for term, score in doc_tfidf.items():
            doc_weights[newFileNames[i]][term] = score

    # Save the document weights to a file
    with open('/Users/hantswilliams/Documents/development/python_projects/clinicaltrials_trec_2022/data/spacy_output/doc_weights.json', 'w') as f:
        json.dump(doc_weights, f)

    # save termweights_df to csv
    termweights_df.to_csv('/Users/hantswilliams/Documents/development/python_projects/clinicaltrials_trec_2022/data/spacy_output/termweights.csv')
    print("termweights saved as termweights.csv and doc_weights saved as doc_weights.json")
    print("\n All Done! \n")

if __name__ == "__main__":
    process_files()

