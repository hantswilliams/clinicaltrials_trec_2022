import json
import pandas as pd
import os
import re
import time
import medspacy
import numpy as np
from scripts_spacy.customStopwords import customStopwords

from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.cluster import KMeans

## setup NLP pipeline
nlp = medspacy.load("en_core_sci_sm")

## add custom stop words
print(nlp.Defaults.stop_words)
for word in customStopwords:
    nlp.Defaults.stop_words.add(word)

def spacy_tokenizer(document):
    nlp.disable_pipes("parser")
    tokens = nlp(document)
    tokens = [token.lemma_ for token in tokens if (
        token.is_stop == False and \
        token.is_punct == False and \
        token.lemma_.strip()!= '')]
    nlp.enable_pipe("parser")
    return tokens

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

## part 1: tokenize and count
count_vectorizer = CountVectorizer(input = 'content', tokenizer = spacy_tokenizer, max_features=150)
count_vectorizer.fit(corpus)
word_count=count_vectorizer.fit_transform(corpus)

## part 2: tf-idf
tfidf_vectorizer = TfidfTransformer(smooth_idf=True, use_idf=True)
tfidf_vectorizer.fit(word_count)
tf_idf_vector=tfidf_vectorizer.transform(word_count)

## get weights across all documents in corpus
weights = np.asarray(tf_idf_vector.mean(axis=0)).ravel().tolist()
weights_df = pd.DataFrame({'term': count_vectorizer.get_feature_names_out(), 'weight': weights})
weights_df = weights_df.sort_values(by='weight', ascending=False)
weights_df.to_csv('./data/spacy_output/weights_test.csv')

## create dataframe
df = pd.DataFrame(tf_idf_vector.toarray(), columns=count_vectorizer.get_feature_names_out())








