import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

# Sample corpus
corpus = [
    "I love to code",
    "Programming is fun",
    "Python is my favorite language",
    "Machine learning is fascinating",
    "Data science is booming",
    "Statistics plays a crucial role",
    "I enjoy solving problems",
    "Algorithms are interesting",
    "Big data is a big deal",
    "Natural language processing is exciting",
    "Deep learning is the future",
    "AI is transforming industries",
    "Computers are powerful tools",
    "I am passionate about technology",
    "Software development is my profession",
    "The internet has changed the world",
    "Cybersecurity is essential",
    "Cloud computing offers scalability",
    "Web development is in high demand",
    "Mobile apps are everywhere",
    "Blockchain has potential",
    "Virtual reality provides immersive experiences",
    "Internet of Things connects devices",
    "Artificial intelligence is revolutionizing",
    "Robotics is advancing rapidly",
    "Quantum computing is on the horizon"
]

# Divide corpus into five blocks
num_blocks = 5
block_size = len(corpus) // num_blocks

corpus_blocks = [corpus[i:i+block_size] for i in range(0, len(corpus), block_size)]

# Process each block independently
tfidf_vectors = []
feature_names = []
for block in corpus_blocks:
    vectorizer = TfidfVectorizer()
    tfidf_matrix = vectorizer.fit_transform(block)
    tfidf_vectors.append(tfidf_matrix)
    feature_names.append(vectorizer.get_feature_names_out())

# Combine the responses
combined_tfidf_matrix = np.vstack(tfidf_vectors)

# Get the feature names across all blocks
combined_feature_names = set()
for names in feature_names:
    combined_feature_names.update(names)

# Get the weighted scores for each term
weighted_scores = {}
for i, term in enumerate(combined_feature_names):
    tfidf_scores = combined_tfidf_matrix[:, i].toarray()
    weighted_scores[term] = np.sum(tfidf_scores)



