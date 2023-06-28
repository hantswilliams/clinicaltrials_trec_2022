# clinicaltrials_trec_2022

## about the data source
- data: https://www.trec-cds.org/2023.html 

## tech
- DB: cloud managed psql: timescale  
- DB management: prisma 
- Languages:
    - analyses: python
    - db management: typescript/js 

## Part 1
- Create a data pipeline:
    - XML -> JSON -> S3 -> CRAWLER/GLUE -> ATHENA 
- Create a API endpoint, lambda, that can be used to analyze (w/ spacy/medspacy/etc..) a new XML file
- Create some type of dashboard 

## MedSpacy
- Different models: https://allenai.github.io/scispacy/ - links from medspacy alternative - scispacy


## part 1 
- load up ec2 instance, download files to that instance (want aws machine because transfer speeds between server and s3 will be quicker)
- run through files on that machine, converting all to json locally there (should take X period of time)
- run through those json files, performing batch upload `s3-parallel-put --put=stupid` () or `aws s3 sync` (10mins) https://stackoverflow.com/questions/42235618/s3-how-to-upload-large-number-of-files 


## ideas
- non-ML: 
    - lots of structured data, e.g., XML fields 
    - first can explore general trends across years based on this data
    - convert XML to JSON->relational DB format, relational since highly structured; each row is a different study 
    - use PRISMA to manage, perhaps planetscale for simplicity 
    - create some visualizations, searchable frontend, dashboard to review those general trends 
    - examples of fixed fields that could be of interest (counts) across time: 
        - sponsors 
        - phase 
        - study type
        - design 
        - outcome 
        - enrollment
        - condition
        - intervention
        - inclusion/exclusion
        - location (country, state, etc...)

    - due to DB size, will probably need to create some views, 

- ML: 
    - look at different langauge models beyond scale (en_core_sci_sm)
        - biobert https://github.com/dmis-lab/biobert 
        - clinicalbert https://github.com/EmilyAlsentzer/clinicalBERT 
        - scispaCy - https://github.com/allenai/scispacy 
    - tokenize, rm stop words, TF-IDF, sort scores; 