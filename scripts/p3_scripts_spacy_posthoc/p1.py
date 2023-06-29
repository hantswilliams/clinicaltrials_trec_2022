import pandas as pd
import boto3

# Load in the data
s3 = boto3.client('s3')
obj = s3.get_object(Bucket='clinicaltrials-gov', Key='idf.csv')
idf_df = pd.read_csv(obj['Body'])

