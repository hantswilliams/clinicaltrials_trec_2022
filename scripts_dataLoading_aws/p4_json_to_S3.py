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
    pool.map(upload_file, file_list)
    
    # Close the pool
    pool.close()
    pool.join()


