# clinicaltrials_trec_2022

## preliminary visualizations / summary: 
- Data dashboard: https://trials.appliedhealthinformatics.com 
- github repo for frontend: https://github.com/hantswilliams/clinicaltrials_trec_2022_frontend

## data source
- data: https://www.trec-cds.org/2023.html 
- original data purpose: clinical trial data. major pain point for clinical trials is recruitment. purpose of this dataset was to combine information related to clinial trials, and a subset of fake patient bios, to try and automatically pair patients with clinical trials. this type of technology is now common, and found in products like TriNetX (currently use this at Stony Brook)

## pre-processing: data transformation and persistance 
- first that data is pre-processed, transformed from XML to JSON 
    - file locations: `scripts/p1_scripts_dataLoading_aws` 
        - p0 script: bash script for downloading
        - p1 script: unzipping 
        - p2 script: helper function 
        - p3 script: xml to json 
        - p4 script: json to s3 
    - as a note, i like json because it is compatable with many new storage technologies which we will use coming up  
- next, because there is some structured data, we take those structure fields from the JSON file push them into athena via the process described in the following line
- after they hit s3, I setup a crawer pipeline to push the json files into lake formation where we can then query it via sql files 
    - this decesion was made to reduce the total amount of words later analyzed, and also because they are structured fields, which we can write queries for, versus performing NLP on structured fields doesn't make sense --> e.g., typically we just perform NLP to provide structure on unstructured things. if we want frequencies for structured things, we can do traditional descriptives. below are some examples of the structured fields that i dont think is helpful to perform the NLP on: 
        - study type
        - design 
        - outcome 
        - enrollment
        - condition
    - in addition, with this approach, the crawler approach could be automatically triggered later with new data being pushed into the bucket (e.g., via streamling data approach or batch)
    - to see some of the athena queries that are then performed and data generated, please see: `./athena` folder with the outputs and queries subfolders; there is a separate readme.md in there

## processing: perform TD / IDF 
- script locations: `scripts/p2_scripts_spacy` 
- there are two scripts in the folder, one that performs the IDF without parallel processing, and one that does 
- based on my description above, the only field within each document that we are performing the analysis on is the `textblock` field which is a free text field, that could benefit from doing the TD / IDF analysis 
- the parallel processing script, not fully optimized, currently takes about ~1 hr to run (3468.879439353943 seconds)
- the output file, is order by weights, descending, with the terms that are most unique/identifying for each document at the top, with the least unique (most common) terms at the bottom: `data/spacy_output/idf.csv` 
- *Future steps*: 
    - in the future to optimize this, the individual documents with their frequency counts should be persisted in a relational database, with the document IDs as the index, and the frequency codes in a json/blob column 
    - when new data comes in, the term frequencies for that one document should be analyzed, added to the db, and then a lambda function/celery/background function running on some machine should then re-calculate the IDFs, and perhaps save those to a new table or review, again, could be within a relational database, or just even a static file somewhere 

