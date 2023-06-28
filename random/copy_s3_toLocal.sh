# copy all files from s3 folder to local folder
aws s3 cp s3://clinicaltrials-gov/ /Users/hantswilliams/Documents/clinicaltrials_trec_2022/s3_bucket/json/ --recursive --exclude "*" --include "*.json" --page-size 1000 --no-paginate

# get count of files in folder
ls -1 | wc -l