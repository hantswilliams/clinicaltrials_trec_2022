import json
import re
import medspacy
from collections import Counter
import boto3

s3_client = boto3.client('s3')
nlp = medspacy.load("en_core_sci_sm")
bucket_name = 'clinicaltrials-gov'

def json_cleaning(text):
    pattern = r"[^\w\s]"
    clean_text = re.sub(pattern, '', text)
    clean_text = clean_text.replace('\n', ' ').replace('\t', ' ').replace('\r', ' ')
    clean_text = ' '.join(clean_text.split())
    return clean_text

def remove_stop_words(doc):
    doc = doc.split()
    doc = [word for word in doc if word not in nlp.Defaults.stop_words]
    return ' '.join(doc)

def calculate_term_frequency(doc):
    doc = doc.split()
    term_frequency = Counter(doc)
    return term_frequency

def lambda_handler(event, context):
    # Retrieve the JSON input from the event
    json_input = json.loads(event['body'])

    # Apply the json_cleaning function to the desired field
    json_input['textblock'][0] = json_cleaning(json_input['textblock'][0])

    # Apply the remove_stop_words function to the cleaned text
    json_input['textblock'][0] = remove_stop_words(json_input['textblock'][0])

    # Calculate the term frequency for the document
    term_frequency = calculate_term_frequency(json_input['textblock'][0])

    # Add the term frequency to the JSON data
    json_input['term_frequency'] = term_frequency

    nct_id = json_input['nct_id']
    output_file_key = f'{nct_id}_processed.json'  # Use the 'nct_id' value as the file key in S3
    output_json_str = json.dumps(json_input)

    # Upload the JSON data to S3 bucket
    s3_client.put_object(
        Body=output_json_str,
        Bucket=bucket_name,
        Key=output_file_key
    )

    # Return the updated JSON data
    return {
        'statusCode': 200,
        'body': json.dumps(json_input)
    }
