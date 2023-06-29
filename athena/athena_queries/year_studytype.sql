WITH subquery AS (
    SELECT CAST(SUBSTRING(study_first_posted[1], -4) AS INT) AS year, study_type[1] AS type
    FROM "AwsDataCatalog"."clinicaltrials_test_1"."clinicaltrials_gov"
    WHERE study_type IS NOT NULL AND CARDINALITY(study_type) > 0
)
SELECT year,
    COUNT(CASE WHEN type = 'Interventional' THEN 1 END) AS Interventional,
    COUNT(CASE WHEN type = 'Observational' THEN 1 END) AS Observational,
    COUNT(CASE WHEN type = 'Other Study Type' THEN 1 END) AS "Other Study Type"
FROM subquery
GROUP BY year;
