import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import decimal
import os
import dotenv

## load in .env file
dotenv.load_dotenv()

session = boto3.Session(
    profile_name='nhit', 
    region_name='us-east-1',
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)

dynamodb = session.resource('dynamodb')

## check which tables exist in the database
def check_tables():
    for table in dynamodb.tables.all():
        print(table.table_name)

check_tables()

with open('temp/NCT00055354.json', 'r') as f:
    jsonData = json.load(f, parse_float=decimal.Decimal)
    jsonData['nct_id'] = jsonData['nct_id'][0]
    print(jsonData)

def put_item_in_database(jsondata):
    table = dynamodb.Table('clinicaltrials')
    table.put_item(Item = jsondata)

put_item_in_database(jsonData)


table = dynamodb.Table('clinicaltrials')
response = table.query(
    KeyConditionExpression=Key('allocation').eq('Randomized')
    )
