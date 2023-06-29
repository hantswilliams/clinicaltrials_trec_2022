# Athena  

## About
- Pipeline: AWS S3 -> Crawler -> Lake Formation -> Athena 
- The data is first processed via the scripts found in `./scripts/p1_scripts_dataLoading_aws`
- A crawler was then setup to parse and find the json data files in the bucket, and send them into lake formation where we can then query them via athena 

## Folders
- athena_queries: just some basic queries setup to get some baseline/descriptive data of the fixed fields contained in the json files
- athena_outputs: this contains the output (in csv form) of the queries to push into some dashboards/frontends/etc...