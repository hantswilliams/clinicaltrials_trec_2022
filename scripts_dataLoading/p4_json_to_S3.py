import os
import boto3
from multiprocessing import Pool

# AWS S3 configuration
bucket_name = 'clinicaltrials-gov'
s3 = boto3.client('s3')

# Folder path containing the files to upload
folder_path = '/home/ubuntu/clinicaltrials_trec_2022/temp/'

def upload_file(file_path):
    # Extract the filename from the file path
    filename = os.path.basename(file_path)
    
    # Upload the file to S3
    s3.upload_file(file_path, bucket_name, filename)
    
    # Optional: Print a message indicating the successful upload
    print(f"Uploaded {filename} to S3")

if __name__ == '__main__':
    # Get the list of files in the folder
    file_list = [os.path.join(folder_path, filename) for filename in os.listdir(folder_path)]
    
    # Create a pool of 4 processes
    pool = Pool(processes=4)
    
    # Use the pool to map the upload_file function to the file list
    pool.map(upload_file, file_list)
    
    # Close the pool
    pool.close()
    pool.join()




# import os

# ## set variables
# folderlocation = '/home/ubuntu/clinicaltrials_trec_2022/temp'
# s3bucketname = 'clinicaltrials-gov'

# ## set commands
# command1 = 'export AWS_PROFILE=nhit'
# command2 = f'aws s3 cp {folderlocation} s3://{s3bucketname} --recursive'

# ## execute commands
# os.system(command1)
# os.system(command2)


##### OR CAN TRY THIS WAY 
## https://stackoverflow.com/questions/42235618/s3-how-to-upload-large-number-of-files
## https://github.com/mishudark/s3-parallel-put/blob/master/s3-parallel-put
## https://fgilio.com/easily-transfer-entire-local-directories-to-amazon-s3-using-s3-parallel-put/ 
## s3-parallel-put --put=stupid --processes=64

## ./s3-parallel-put --put=stupid --processes=4 --bucket=clinicaltrials-gov /home/ubuntu/clinicaltrials_trec_2022/temp/