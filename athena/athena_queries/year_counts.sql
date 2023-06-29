
SELECT CAST(SUBSTRING(study_first_posted[1], -4) AS INT) AS year, COUNT(*) AS count
FROM "AwsDataCatalog"."clinicaltrials_test_1"."clinicaltrials_gov"
GROUP BY CAST(SUBSTRING(study_first_posted[1], -4) AS INT);