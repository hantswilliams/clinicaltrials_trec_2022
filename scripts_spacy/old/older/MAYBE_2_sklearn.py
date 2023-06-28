from multiprocessing import Pool
from scipy import sparse
import spacy
import medspacy
import functools
import json
import os
import re

# Set up medspaCy and Spacy tokenizer
medspacy.load("en_core_sci_sm")
nlp = medspacy.load("en_core_sci_sm")
                    
# Load the pre-trained word vectors
word_vectors_model = spacy.load("en_core_sci_sm")
word_vectors = word_vectors_model.vocab

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
        token.lemma_.strip() != '' and \
        token.text in word_vectors)]
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

# Set the number of processes to run in parallel
num_processes = 4  # You can adjust this value based on your system's capabilities

# Define the minimum number of features
min_features = 5

# Define the maximum number of features
max_features = 150

# Define a class to encapsulate the process_chunk method
class ChunkProcessor:
    def __init__(self, tokenizer, min_features, max_features):
        self.tokenizer = tokenizer
        self.min_features = min_features
        self.max_features = max_features
        self.vocab = set()

    def process_chunk(self, chunk):
        # Tokenize the documents in the chunk
        tokenized_docs = [self.tokenizer(doc) for doc in chunk]

        # Update the vocabulary with tokens from the current chunk
        for doc_tokens in tokenized_docs:
            self.vocab.update(doc_tokens)

        # Filter out documents that don't meet the minimum number of features
        filtered_chunk = [doc for doc_tokens, doc in zip(tokenized_docs, chunk) if len(doc_tokens) >= self.min_features]

        # Create sparse matrix representations of the filtered documents
        word_count_vectors = []
        for doc_tokens in filtered_chunk:
            # Count the occurrences of each token in the document
            word_counts = {token: doc_tokens.count(token) for token in doc_tokens if token in self.vocab}

            # Sort the tokens alphabetically and take the maximum number of features
            sorted_tokens = sorted(word_counts.keys())
            selected_tokens = sorted_tokens[:self.max_features]

            # Create a sparse matrix representation of the document
            row = [0] * len(selected_tokens)
            col = [sorted_tokens.index(token) for token in selected_tokens]
            data = [word_counts[token] for token in selected_tokens]
            word_count_vectors.append((row, col, data))

        return word_count_vectors

def main():
    # Split the corpus into chunks
    chunk_size = len(corpus) // num_processes
    chunks = [corpus[i:i+chunk_size] for i in range(0, len(corpus), chunk_size)]

    # Create an instance of ChunkProcessor
    processor = ChunkProcessor(tokenizer=spacy_tokenizer, min_features=min_features, max_features=max_features)

    # Create a partial function for process_chunk
    process_chunk_partial = functools.partial(processor.process_chunk)

    # Create a Pool of worker processes
    with Pool(num_processes) as pool:
        # Process each chunk in parallel
        word_count_vectors_list = pool.map(process_chunk_partial, chunks)

    # Combine the results from each chunk
    word_count_vectors = []
    for vectors_list in word_count_vectors_list:
        for vector in vectors_list:
            word_count_vectors.append(vector)

    # Create the final sparse matrix representation
    num_documents = len(word_count_vectors)
    num_features = len(processor.vocab)

    print('Number of documents:', num_documents)
    print('Number of features:', num_features)

    row = []
    col = []
    data = []
    for doc_index, (doc_row, doc_col, doc_data) in enumerate(word_count_vectors):
        for i in range(len(doc_row)):
            # Check if token exists in the vocabulary
            if doc_col[i] in processor.vocab:
                row.append(doc_index)
                col.append(doc_col[i])
                data.append(doc_data[i])

    word_count_matrix = sparse.csr_matrix((data, (row, col)), shape=(num_documents, num_features))

    # Print the final result or perform additional actions
    print(word_count_matrix)

    ## Get the feature names along with their weights
    feature_names = [token for token in processor.vocab]
    feature_weights = word_count_matrix.sum(axis=0).tolist()[0]
    features = sorted(zip(feature_names, feature_weights), key=lambda x: x[1], reverse=True)
    print(features)



if __name__ == '__main__':
    main()

