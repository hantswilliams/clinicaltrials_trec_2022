##     pip install --upgrade gensim

from gensim.parsing.preprocessing import remove_stopwords

sample_text = "Oh man, this is pretty cool. We will do more such things."
sample_text_NSW = remove_stopwords(text)

print(word_tokenize(sample_text))
print(word_tokenize(sample_text_NSW))