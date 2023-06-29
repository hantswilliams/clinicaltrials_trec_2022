## Notes

### TF 
- Currently TF is set, but no data is pushed back into athena 
- To make this more effective, should probably update the JSON file in S3 with the new TF infomation 
- The TF could be pushed back into the json file into S3, and then detected by the web crawler 

### IDF 
- This needs to be optimized 
- There should probably be another table that contains the TF, each row is a different record (trial) + TF 
- Approach could then be performed via SQL perhaps? writting script that performs the IDF analysis 
- Would potneitally have two tables: 
    a. Term table 

        | TermID | Term      |
        |--------|-----------|
        | 1      | apple     |
        | 2      | banana    |
        | 3      | cat       |
        | 4      | dog       |
        | 5      | elephant  |
        | ...    | ...       |

    b. Document-Term table 

        | DocumentID | TermID | Count |
        |------------|--------|-------|
        | 1          | 1      | 5     |
        | 1          | 2      | 2     |
        | 1          | 3      | 0     |
        | 1          | 4      | 3     |
        | 1          | 5      | 1     |
        | 2          | 1      | 0     |
        | 2          | 2      | 1     |
        | 2          | 3      | 4     |
        | 2          | 4      | 2     |
        | 2          | 5      | 0     |
        | ...        | ...    | ...   |
