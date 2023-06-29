import pandas as pd
import numpy as np
import medspacy
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import CountVectorizer

import os
import json
import re
import time
from multiprocessing import Pool
import functools
from scipy.sparse import vstack
from scipy.sparse import hstack
from scipy import sparse





## setup NLP pipeline
nlp = medspacy.load("en_core_sci_sm")

docs=["the house had a tiny little mouse",
      "the cat saw the mouse",
      "the mouse ran away from the house",
      "the cat finally ate the mouse",
      "the end of the mouse story"
     ]

directory = "./s3_bucket/json/"
fileNames = os.listdir(directory)

def json_cleaning(text):
    pattern = r"[^\w\s]"
    clean_text = re.sub(pattern, '', text)    
    clean_text = clean_text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
    clean_text = ' '.join(clean_text.split())
    return clean_text

# Define the tokenization function
def spacy_tokenizer(document):
    nlp.disable_pipes("parser")
    tokens = nlp(document)
    tokens = [token.lemma_ for token in tokens if (
        token.is_stop == False and \
        token.is_punct == False and \
        token.lemma_.strip() != '')]
    nlp.enable_pipe("parser")
    return tokens

## load first X files from fileNames into a list of strings, just the textblock
corpus = []
fileCount = 1000
for i in range(fileCount):
    with open(directory + '/' + fileNames[i], 'r') as f:
        try:
            jsonData = json.load(f)
            jsonData['textblock'][0] = json_cleaning(jsonData['textblock'][0])
            corpus.append(jsonData['textblock'][0])
        except:
            print("Error loading file: " + fileNames[i])
            continue





# Set the number of processes to run in parallel
num_processes = 4  # You can adjust this value based on your system's capabilities
min_features = 50  # Minimum number of features (words) required to select a document

# Create a representative sample of documents for fitting the CountVectorizer
sample_size = 100  # Adjust the sample size as needed
sample = corpus[:sample_size]

# Instantiate CountVectorizer()
cv = CountVectorizer(input='content', tokenizer=spacy_tokenizer, min_df=min_features, max_features=100)

# Fit the CountVectorizer on the sample
print("Fitting CountVectorizer on {} documents...".format(sample_size))
cv.fit(sample)
print("CountVectorizer fitted")

# Define a class to encapsulate the process_chunk method
class ChunkProcessor:
    def __init__(self, vectorizer):
        self.vectorizer = vectorizer

    def process_chunk(self, chunk):
        # Filter out documents that don't meet the minimum number of features
        filtered_chunk = [doc for doc in chunk if len(spacy_tokenizer(doc)) >= min_features]
        return self.vectorizer.transform(filtered_chunk)

def main():
    # Split the corpus into chunks
    chunk_size = len(corpus) // num_processes
    chunks = [corpus[i:i+chunk_size] for i in range(0, len(corpus), chunk_size)]

    # Create an instance of ChunkProcessor
    processor = ChunkProcessor(cv)

    # Create a partial function for process_chunk
    process_chunk_partial = functools.partial(processor.process_chunk)

    # Create a Pool of worker processes
    with Pool(num_processes) as pool:
        # Process each chunk in parallel
        word_count_vectors = pool.map(process_chunk_partial, chunks)

    # Get the number of features
    num_features = word_count_vectors[0].shape[1]

    # Combine the results from each chunk
    for i in range(1, len(word_count_vectors)):
        if word_count_vectors[i].shape[1] < num_features:
            word_count_vectors[i] = hstack(
                [word_count_vectors[i], sparse.csr_matrix((word_count_vectors[i].shape[0], num_features - word_count_vectors[i].shape[1]))]
            )
        elif word_count_vectors[i].shape[1] > num_features:
            word_count_vectors[i] = word_count_vectors[i][:, :num_features]

    word_count_vector = hstack(word_count_vectors)
    print("Combined word count vector shape: {}".format(word_count_vector.shape))

if __name__ == '__main__':
    main()






































# #instantiate CountVectorizer() // this is the part that need to optimze
# cv=CountVectorizer(input = 'content', tokenizer = spacy_tokenizer, max_features=150)
# word_count_vector=cv.fit_transform(corpus)

# print(word_count_vector.shape)
# tokens = cv.get_feature_names_out()
# print(tokens)
# print(len(tokens))
# print(word_count_vector.toarray())

# doc_names = ['Doc{:d}'.format(idx) for idx, _ in enumerate(word_count_vector)]

# df = pd.DataFrame(data=word_count_vector.toarray(), index=doc_names,
#                   columns=tokens)

# tfidf_transformer=TfidfTransformer(smooth_idf=True,use_idf=True)
# tfidf_transformer.fit(word_count_vector)

# ### TOTAL WEIGHTS ACROSS EVERYTHING
# df_idf = pd.DataFrame(tfidf_transformer.idf_, index=tokens,columns=["idf_weights"])
# df_idf.sort_values(by=['idf_weights'], ascending=False)

# # tf-idf scores // individual doc 
# # count matrix
# count_vector=cv.transform(docs)
# tf_idf_vector=tfidf_transformer.transform(count_vector)
# first_document_vector=tf_idf_vector[0]
# df = pd.DataFrame(first_document_vector.T.todense(), index=tokens, columns=["tf-idf"])
# df.sort_values(by=["tf-idf"],ascending=False)