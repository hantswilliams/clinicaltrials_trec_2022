import json
import pandas as pd
import os
import re
import time
import medspacy
from scripts_spacy.old.customStopwords import customStopwords
from tqdm import tqdm
import functools

from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.feature_extraction.text import TfidfTransformer
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
fileCount = 100
for i in range(fileCount):
    with open(directory + '/' + fileNames[i], 'r') as f:
        try:
            jsonData = json.load(f)
            jsonData['textblock'][0] = json_cleaning(jsonData['textblock'][0])
            corpus.append(jsonData['textblock'][0])
        except:
            print("Error loading file: " + fileNames[i])
            continue





##### TF-IDF
starttime = time.time()
tfidf_vectorizer = TfidfVectorizer(input = 'content', tokenizer = spacy_tokenizer, max_features=150)
result = tfidf_vectorizer.fit_transform(corpus) #this is the part that takes a long time

print(result)
tfidf_vectorizer.get_feature_names_out()
len(tfidf_vectorizer.get_feature_names_out())

## IDF transformer  // Computing Inverse Document Frequency (WEIGHTS)
tfidf_transformer=TfidfTransformer(smooth_idf=True,use_idf=True)
tfidf_transformer.fit(result)
df_idf = pd.DataFrame(tfidf_transformer.idf_, index=tfidf_vectorizer.get_feature_names_out(),columns=["idf_weights"])
df_idf.reset_index(inplace=True)
df_idf.rename(columns = {'index':'word'}, inplace = True)
df_idf = df_idf.sort_values(by=['idf_weights'], ascending=False)
endtime = time.time()
print("Time to run TF-IDF: " + str(endtime - starttime))
### current setup here is about 5.5 hours if were to do 400,000 files; 
### took 46seconds to run 1k files

## TF-IDF vectors across all documents 
dense = result.todense()
denselist = dense.tolist()
df = pd.DataFrame(denselist,columns=tfidf_vectorizer.get_feature_names_out())

## push all tfidf_vector data into a single column that is json
df.insert(0, 'file_name', fileNames[0:fileCount])
df.insert(1, 'tfidf_vector', df.apply(lambda x: x.to_json(), axis=1))
df.drop(df.columns.difference(['file_name', 'tfidf_vector']), 1, inplace=True)

#### save to a db in future 
df_idf.to_csv('data/spacy_output/idf_weights_v1.csv', index=True)
df.to_csv('data/spacy_output/tfidf_vectors_df.csv', index=False)







## Exploring TF-IDF vectors within a specific document [X]
document = 0
tf_idf_vector = tfidf_transformer.transform(result)
feature_names = tfidf_vectorizer.get_feature_names_out()
first_document_vector = tf_idf_vector[document]
df_tfifd = pd.DataFrame(first_document_vector.T.todense(), index=feature_names, columns=["tfidf"])
df_tfifd.sort_values(by=["tfidf"],ascending=False)







## get the max tf-idf score for each document
max_tfidf = result.max(axis=1).toarray().ravel()
sorted_by_tfidf = max_tfidf.argsort()
feature_names = tfidf_vectorizer.get_feature_names_out()
print("Features with lowest tfidf:\n{}".format(
        feature_names[sorted_by_tfidf[:20]]))

## get the lowest tf-idf score for each document
min_tfidf = result.min(axis=1).toarray().ravel()
sorted_by_tfidf = min_tfidf.argsort()
feature_names = tfidf_vectorizer.get_feature_names_out()
print("Features with lowest tfidf:\n{}".format(
        feature_names[sorted_by_tfidf[:20]]))



##### GROUPING SIMILAR DOCUMENTS
## K-means clustering
kmeans = KMeans(n_clusters=3).fit(result)
kmeans.labels_









