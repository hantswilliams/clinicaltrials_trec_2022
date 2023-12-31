import math
import os
import json
import pandas as pd
import time
from tqdm import tqdm
from concurrent import futures
import multiprocessing
import re
import medspacy
import boto3
import pickle

######## SIMPLIFIED STEPS ########
#### Part 1
# 1 load in json file
# 2 tokenize
# 3 calculate term frequency
#### Part 2
#  calculate IDF 
########################


########################################################################
#########################  Part 1   ####################################
########################################################################

### Helper Functions ###
def json_cleaning(text):
    pattern = r"[^\w\s]"
    clean_text = re.sub(pattern, '', text)    
    clean_text = clean_text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
    clean_text = clean_text.lower()
    clean_text = ' '.join(clean_text.split())
    return clean_text

nlp = medspacy.load("en_core_sci_sm")

## capture start time
starttime = time.time()

## load in json files
filelist = os.listdir('./s3_bucket/json/')
# filelist = filelist[:100]

## define the function to process a single file
def process_file(file):
    try:
        with open('./s3_bucket/json/' + file, 'r') as f:
            jsonData = json.load(f)
            doc = json_cleaning(jsonData['textblock'][0])
    except:
        print("Error loading file: " + file)
        return None  # Return None if error occurs
    
    ## Step 2: Tokenize and clean
    tokenized_doc = doc.split()  # Tokenize the document
    tokenized_doc = [word for word in tokenized_doc if word not in nlp.Defaults.stop_words]
    
    ## Step 3: Calculate term frequency
    term_freq = {term: tokenized_doc.count(term) for term in set(tokenized_doc)}

    return term_freq

## create empty list for term frequency dictionaries
term_frequencies = []

## initialize the progress bar with the total number of files
progress_bar = tqdm(filelist, desc="Processing files", unit="file")

## parallel processing
with futures.ProcessPoolExecutor() as executor:
    ## loop through each filelist item and submit the tasks to the executor
    future_results = [executor.submit(process_file, file) for file in filelist]

    ## wait for all futures to complete
    for future in futures.as_completed(future_results):
        term_freq = future.result()

        if term_freq is not None:
            term_frequencies.append(term_freq)

        # update the progress bar
        progress_bar.set_postfix(completed=f"{progress_bar.n}/{progress_bar.total}")
        progress_bar.update(1)

progress_bar.close()

## Count the total number of documents processed
total_documents = len(term_frequencies)
print(f"Total documents processed: {total_documents}")


########################################################################
#########################  Part 2   ####################################
########################################################################

# Calculate IDF for a given term
# number of terms found within each doc
all_terms = set()
for term_freq in term_frequencies:
    all_terms.update(term_freq.keys())

def calculate_idf(term):
    doc_count = sum(1 for term_freq in term_frequencies if term in term_freq)
    return term, math.log(total_documents / (1 + doc_count))

# create a shared queue to store the results
results_queue = multiprocessing.Queue()

# Use parallel processing to calculate IDF
with multiprocessing.Pool() as pool:
    # Apply async function to map the tasks
    results = [pool.apply_async(calculate_idf, args=(term,), callback=results_queue.put) for term in all_terms]

    # Create a tqdm progress bar
    progress_bar = tqdm(total=len(all_terms))

    # Keep track of completed tasks
    completed_tasks = 0

    # Update progress bar as results become available
    while completed_tasks < len(all_terms):
        while not results_queue.empty():
            results_queue.get()
            completed_tasks += 1
            progress_bar.update(1)

    # Get the results from the async tasks
    idf = {term: result.get()[1] for term, result in zip(all_terms, results)}

    # Close the progress bar
    progress_bar.close()

idf_df = pd.DataFrame.from_dict(idf, orient="index", columns=["idf"])
idf_df = idf_df.sort_values(by="idf", ascending=False)
print(f"IDF dataframe sorted by IDF value: {idf_df.head(10)}")
print(f"IDF dataframe sorted by IDF value: {idf_df.tail(10)}")

## Create TF-IDF dictionary that shows each term in each document
tf_idf = []
for term_freq in term_frequencies:
    tf_idf.append({term: tf * idf[term] for term, tf in term_freq.items()})


########################################################################
#########################  Saving   ####################################
########################################################################

## save output to csv for idf weights locally
print("Saving output to csv...")
try:
    idf_df.to_csv('./data/spacy_output/idf.csv')
    print("idf_df Output saved to csv")
except:
    print("idf_df Error saving to csv")

## idf_df to s3
print("Saving output to s3...")
try:
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('./data/spacy_output/idf.csv', 'clinicaltrials-gov', 'idf.csv')
    print("idf_df Output saved to s3")
except:
    print("idf_df Error saving to s3")

## save tf_idf with pickle locally 
print("Saving output to pickle...")
try:
    with open('./data/spacy_output/tf_idf.pickle', 'wb') as f:
        pickle.dump(tf_idf, f)
    print("Pickle Output saved to pickle")
except:
    print("Pickle Error saving to pickle")

## tf_idf pickle to s3
print("Saving output to s3...")
try:
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('./data/spacy_output/tf_idf.pickle', 'clinicaltrials-gov', 'tf_idf.pickle')
    print("Pickle Output saved to s3")
except:
    print("Pickle Error saving to s3")

## Print the total time taken
print(f"Total time taken: {time.time() - starttime} seconds")