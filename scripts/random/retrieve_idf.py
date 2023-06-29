import boto3
import pandas as pd

## open s3 connection to get idf.csv

s3 = boto3.client('s3')
s3.download_file('clinicaltrials-gov', 'idf.csv', './data/spacy_output/idf.csv') 

## read idf.csv into dataframe
idf_df = pd.read_csv('./data/spacy_output/retrieved_s3_idf.csv', index_col=0)

## download tf_idf.pickle from s3
s3.download_file('clinicaltrials-gov', 'tf_idf.pickle', './data/spacy_output/tf_idf.pickle')