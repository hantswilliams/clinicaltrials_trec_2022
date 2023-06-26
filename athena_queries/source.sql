SELECT source, COUNT(*) AS count
FROM "AwsDataCatalog"."clinicaltrials_test_1"."clinicaltrials_gov"
GROUP BY source;