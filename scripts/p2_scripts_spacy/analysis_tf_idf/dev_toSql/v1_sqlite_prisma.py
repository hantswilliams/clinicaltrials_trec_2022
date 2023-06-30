## testing file for pushing into a new temp db locally, to test the speed of the process
## for DF / IDF calculations

from sqlalchemy import text
import sqlite3
import medspacy
import json
import tqdm
import sys
import boto3
import dotenv 
import pandas as pd
import time

dotenv.load_dotenv()

sys.path.insert(1, '/Users/hantswilliams/Documents/development/python_projects/clinicaltrials_trec_2022')
from scripts.functions.json_cleaning import json_cleaning

## S3 file list
with open('./data/fileList_s3.json') as json_file:
    fileList_s3 = json.load(json_file)
fileList_s3 = fileList_s3[0:1000]

#########################################################################################################
#########################################################################################################

nlp = medspacy.load("en_core_sci_sm")

# Connect to sqlite3 database managed via prisma
conn = sqlite3.connect('./prisma/dev.db')
c = conn.cursor()

#########################################################################################################
#########################################################################################################

# Create progress bar and loop through the list
pbar = tqdm.tqdm(total=len(fileList_s3))
starttime = time.time()
for file in fileList_s3:

    try:
        doc = boto3.client('s3').get_object(Bucket='clinicaltrials-gov', Key=file.replace('s3://clinicaltrials-gov/', ''))
        doc = json.loads(doc['Body'].read().decode('utf-8'))
        doc = doc['textblock'][0]
    except:
        print("Error loading file: " + file)
        continue

    # Clean the file
    data = json_cleaning(doc)

    # Tokenize the document / remove stop words
    tokenized_doc = data.split()  # Tokenize the document
    tokenized_doc = [word for word in tokenized_doc if word not in nlp.Defaults.stop_words]

    # Calculate term frequency
    term_freq = {term: tokenized_doc.count(term) for term in set(tokenized_doc)}

    # Create a term list
    terms = list(term_freq.keys())

    # Execute a single query to check if terms exist
    result = conn.execute("SELECT id, term FROM terms WHERE term IN ({})".format(','.join(['?'] * len(terms))), terms)

    # Fetch existing term IDs from the result
    existing_terms = {row[1]: row[0] for row in result}

    # Insert new terms for those that don't exist
    new_terms = []
    for term in terms:
        if term not in existing_terms:
            new_terms.append(term)

    if new_terms:
        new_term_ids = {}
        for term in new_terms:
            query = "INSERT INTO terms (term) VALUES ('" + term + "') RETURNING term, id"
            result = conn.execute(query)
            row = result.fetchone()
            new_term_ids[row[0]] = row[1]

    # Combine existing and new term IDs
    term_ids = {**existing_terms, **new_term_ids} if new_terms else existing_terms

    # Insert rows into DocumentTerm table via DF to_sql to speed up the process of inserting rows
    # into the table via 1 query for the DF versus 1 query per row
    documentID = file.replace(".json", "").replace("s3://clinicaltrials-gov/", "")
    insert_rows = []

    for term, freq in term_freq.items():
        term_id = term_ids.get(term)
        if term_id is not None:
            insert_rows.append({'DocumentID': documentID, 'TermID': term_id, 'Count': freq})

    df = pd.DataFrame(insert_rows)
    
    ## loop through the df and insert into the sqlite db
    for index, row in df.iterrows():
        c.execute('INSERT INTO documentterm (document, termId, count) VALUES (?, ?, ?)', (row['DocumentID'], row['TermID'], row['Count']))
        conn.commit()

    # update the progress bar
    pbar.update(1)

# close the progress bar
pbar.close()
endtime = time.time()

print("data files parsed and saved...in " + str(round(endtime - starttime, 2)) + " seconds")

## close the connection
conn.close()