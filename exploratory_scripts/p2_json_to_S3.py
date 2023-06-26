import os

## set variables
folderlocation = '/Users/hantswilliams/Documents/development/python_projects/clinicaltrials_trec_2022/temp'
s3bucketname = 'clinicaltrials-gov'

## set commands
command1 = 'export AWS_PROFILE=nhit'
command2 = f'aws s3 cp {folderlocation} s3://{s3bucketname} --recursive'

## execute commands
os.system(command1)
os.system(command2)
