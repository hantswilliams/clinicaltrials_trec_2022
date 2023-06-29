SELECT CAST(SUBSTRING(study_first_posted[1], -4) AS INT) AS year, source, COUNT(*) AS count
FROM "AwsDataCatalog"."clinicaltrials_test_1"."clinicaltrials_gov"
WHERE study_first_posted IS NOT NULL
GROUP BY CAST(SUBSTRING(study_first_posted[1], -4) AS INT), source;
