# clinicaltrials_trec_2022

## about the data source
- data: https://www.trec-cds.org/2023.html 

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