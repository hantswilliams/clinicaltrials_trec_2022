#### RAndom Notes 

## installing models: pip install https://s3-us-west-2.amazonaws.com/ai2-s2-scispacy/releases/v0.5.1/en_core_sci_sm-0.5.1.tar.gz
## https://allenai.github.io/scispacy/ 

## https://oyewusiwuraola.medium.com/how-to-use-scispacy-entity-linkers-for-biomedical-named-entities-7cf13b29ef67
## For UMLS --> https://github.com/Georgetown-IR-Lab/QuickUMLS 
## MedSpacy - Quick UMLS -> https://pypi.org/project/medspacy-quickumls/
## Alt: SciSpacy https://allenai.github.io/scispacy/ --> has good language models for download // en_core_sci_lg -> 785k words, 600k word vectors trained on biomedical data

## nice examples of target rules for COVID-19: https://github.com/abchapman93/VA_COVID-19_NLP_BSV
## example notebooks, learning, ideas: https://github.com/Melbourne-BMDS/mimic34md2020_materials/tree/master
## another nice tutorial: https://www.andrewvillazon.com/clinical-natural-language-processing-python/ 

## perhaps come up with some search term/flag words that would related to biochecicalnucleur threats, 
## potenially of interest to the department of homeland security
##### inspired by: https://github.com/abchapman93/ReHouSED 
### Potnetial search terms: https://www.nti.org/education-center/glossary/
## Mesh Terms: 
# ## - WMDs: https://www.ncbi.nlm.nih.gov/mesh/68054044 has bio, chem, nuclear
# ## - Biological - https://www.ncbi.nlm.nih.gov/mesh/68054045 (has 20+ entity terms)
# ## - Checmical - https://www.ncbi.nlm.nih.gov/mesh/68002619
# ## - Nuclear - https://www.ncbi.nlm.nih.gov/mesh/68054043 
### CDC: https://www.cdc.gov/eis/field-epi-manual/chapters/Biologic-Toxic-Agents.html 


#### basics of term frequency / inverse docuemnt frequency
## https://towardsdatascience.com/lovecraft-with-natural-language-processing-part-3-tf-idf-vectors-8c2d4df98621 
## https://tfidf.com/ 
## https://www.analyticsvidhya.com/blog/2021/09/creating-a-movie-reviews-classifier-using-tf-idf-in-python/
## http://www.sefidian.com/2022/07/28/understanding-tf-idf-with-python-example/ 
## https://python.plainenglish.io/text-classification-using-python-spacy-7a414abcc83a
## https://www.kaggle.com/code/satishgunjal/tutorial-text-classification-using-spacy

# https://www.capitalone.com/tech/machine-learning/understanding-tf-idf/

#### Additional stop words to add should be the names of the columns that are common: 
### can go back to this doc: https://github.com/hantswilliams/clinicaltrials_trec_2022/blob/3aa29cc3774aad0aef7cd42de87fdab86c03a1af/prisma/schema.prisma
## or potentially repull those terms that are common, e.g., all of the keys in the json files







