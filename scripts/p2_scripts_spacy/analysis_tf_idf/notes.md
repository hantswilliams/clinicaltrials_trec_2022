## Notes

### TF and IDF 
- Currently TF is set, but no data is pushed back into athena 
- To make this more effective, could do a combination of things, such as updating the JSON file in S3 with the new TF infomation which will then be consumed by crawler/athena, or could send it directly to a relational DB, creating a term table and document-term table as describe below  

### IDF 
- Optimization of this thinking for now should be SQL - two tables: (1) term table and (2) term-document-count table
- Could then use a sql query to perform the IDF analysis  
- So would potentially have two tables: 
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

- would then be able to use a query such as below, to compute IDF very quickly (?):
    ```
    CREATE VIEW IDFView AS
    SELECT
        t.TermID,
        t.Term,
        LOG10(CAST(COUNT(*) AS FLOAT) / (SELECT COUNT(DISTINCT DocumentID) FROM DocumentTerm)) AS IDF
    FROM
        Terms t
    JOIN
        DocumentTerm dt ON t.TermID = dt.TermID
    GROUP BY
        t.TermID, t.Term;
    ```