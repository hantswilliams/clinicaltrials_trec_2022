
import math
from collections import Counter
import time
import pandas as pd
import medspacy
import spacy

import os
import json
import re

def json_cleaning(text):
    pattern = r"[^\w\s]"
    clean_text = re.sub(pattern, '', text)    
    clean_text = clean_text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
    clean_text = ' '.join(clean_text.split())
    return clean_text

##### Data files 
directory = "./s3_bucket/json/"
fileNames = os.listdir(directory)

## load first X files from fileNames into a list of strings, just the textblock
documents = []
filesNotLoaded = []
fileCount = 100
for i in range(fileCount):
    with open(directory + '/' + fileNames[i], 'r') as f:
        try:
            jsonData = json.load(f)
            jsonData['textblock'][0] = json_cleaning(jsonData['textblock'][0])
            documents.append(jsonData['textblock'][0])
        except:
            print("Error loading file: " + fileNames[i])
            filesNoteLoaded = filesNotLoaded.append(fileNames[i])
            continue

newFileCount = len(documents)
newFileNames = fileNames[0:newFileCount]

nlp = medspacy.load("en_core_sci_sm")

# Tokenize and count the words in each document
tokenized_documents = [doc.split() for doc in documents]
print('gut check: total words: ', sum(len(doc) for doc in tokenized_documents))

# Remove stop words from each document
nlp.disable_pipes("parser")  # Disable the parser component
for i, doc in enumerate(tokenized_documents):
    print('processing document: ', i)
    tokenized_documents[i] = [word for word in doc if word not in nlp.Defaults.stop_words]
print('gut check: total words: ', sum(len(doc) for doc in tokenized_documents))

# Calculate the term frequency for each document
term_frequency = [Counter(doc) for doc in tokenized_documents]

# Calculate the term frequency for each term in each document
tf = []
for doc in term_frequency:
    doc_tf = {}
    total_terms = sum(doc.values())
    for term, freq in doc.items():
        doc_tf[term] = freq / total_terms
    tf.append(doc_tf)

# Combine all the documents into one list
all_terms = set()
for doc in tokenized_documents:
    all_terms.update(doc)

# Calculate the inverse document frequency for each term
idf = {}
total_documents = len(documents)
starttime = time.time()
for term in all_terms:
    doc_count = sum(1 for doc in tokenized_documents if term in doc)
    idf[term] = math.log(total_documents / (1 + doc_count))
endtime = time.time()
print("Time to calculate IDF: " + str(endtime - starttime))


## for 1k, it took 13 seconds to run - so about 90 minutes for 400k files
## for 5k, it took 211 seconds to run - so about 280 minutes for 400k files

# Calculate the TF-IDF for each term in each document
tf_idf = [] # CONTAINS ALL THE TF-IDF SCORES
for doc_tf in tf:
    doc_tfidf = {}
    for term, tf_score in doc_tf.items():
        doc_tfidf[term] = tf_score * idf[term]
    tf_idf.append(doc_tfidf)

# Limit the number of terms per document
max_terms_per_document = 150 # CHANGE THIS TO LIMIT THE NUMBER OF TERMS PER DOCUMENT 150 
limited_tf_idf = [] # CONTAINS ALL THE TF-IDF SCORES, LIMITED TO max_terms_per_document
for doc_tfidf in tf_idf:
    sorted_terms = sorted(doc_tfidf.items(), key=lambda x: x[1], reverse=True)[:max_terms_per_document]
    limited_tf_idf.append(dict(sorted_terms))

# Print the TF-IDF scores for each document
for i, doc_tfidf in enumerate(limited_tf_idf):
    print(f"Document {i+1}:")
    for term, score in doc_tfidf.items():
        print(f"{term}: {score}")
    print()

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

# # Create a dataframe of the TF-IDF scores for each document, where the index is the document number
tfidf_df = pd.DataFrame(limited_tf_idf)
tfidf_df['filename'] = newFileNames[0:newFileCount]
tfidf_df.shape