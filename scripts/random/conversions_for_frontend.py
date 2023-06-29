import pandas as pd

# Load in the data
source_year = pd.read_csv('./athena/athena_outputs/source_by_year.csv')
trials_year = pd.read_csv('./athena/athena_outputs/trials_by_year.csv')
trials_sources = pd.read_csv('./athena/athena_outputs/trials_sources.csv')
types_year = pd.read_csv('./athena/athena_outputs/types_by_year.csv')
idf = pd.read_csv('./data/spacy_output/idf.csv')



###### DATA CLEANING ######

#1. for trials_sources and source_year, for the column source, remove [ and ] brackets
def remove_brackets(x):
    return x[1:-1]

trials_sources['source'] = trials_sources['source'].apply(remove_brackets)
source_year['source'] = source_year['source'].apply(remove_brackets)

#2. for types_year, remove the last column (other study type)
types_year = types_year.iloc[:, :-1]

#3. sort types_year by year, ascending
types_year = types_year.sort_values(by='year', ascending=True)

#4. sort source_year by year, ascending
source_year = source_year.sort_values(by='year', ascending=True)

#5. sort trials_year by year, ascending
trials_year = trials_year.sort_values(by='year', ascending=True)

#6. for idf, rename the first column to term
idf = idf.rename(columns={'Unnamed: 0': 'term'})


###### NEW DATA FRAME CREATIONS ######

#6. Create a new dataframe which is the total count of all trials by source across all years
trials_sources_total = trials_sources.groupby(['source']).sum().reset_index()
trials_sources_total = trials_sources_total.sort_values(by='count', ascending=False)
trials_sources_total = trials_sources_total.iloc[:100, :]



###### DATA FRAME OUTPUTS AS JSON ######
#0. trials_sources
trials_sources.to_json('./data/frontend/trials_sources.json', orient='records')
#1. trials_sources_total
trials_sources_total.to_json('./data/frontend/trials_sources_total.json', orient='records')
#2. source_year
source_year.to_json('./data/frontend/source_year.json', orient='records')
#3. trials_year
trials_year.to_json('./data/frontend/trials_year.json', orient='records')
#4. types_year
types_year.to_json('./data/frontend/types_year.json', orient='records')
#5. idf
idf.to_json('./data/frontend/idf.json', orient='records')