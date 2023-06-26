import json 
import os 
import sqlalchemy
import dotenv
import uuid

## load in .env file
dotenv.load_dotenv()

## connect to db found in .env file, which is a postgresql db
engine = sqlalchemy.create_engine(os.getenv('DATABASE_URL_SQLALCHEMY'))

## test the connection, get a list of tables
engine.table_names()

## query the rows from the Trials table
engine.execute('SELECT * FROM trials LIMIT 5').fetchall()

## get list of files that exist in /temp
files = os.listdir('temp')

## create a list of all the keys found in all the json files
keys = []

## loop through each file, load in the json, get the keys, and append to keys list
for i in range(len(files)):
    file_name = files[i]
    with open(f"temp/{files[i]}", "r") as f:
        json_content = json.load(f)
    keys.append(list(json_content.keys()))

keys = [item for sublist in keys for item in sublist]
len(keys)

## remove duplicates
keys = list(set(keys))
len(keys)

## figure out the names of the columns to create 
columnnames = keys

## sort column names alphabetically
columnnames.sort()

for i in range(len(files)):
    ## load in first json file from list
    with open(f"temp/{files[i]}", "r") as f:
        json_content = json.load(f)

        print('nct_id: ', json_content['nct_id'][0])

        ## check if the nct_id already exists in the database table Trials if it does, then skip it
        query = f"SELECT EXISTS(SELECT 1 FROM trials WHERE nct_id = '{json_content['nct_id'][0]}')"

        if engine.execute(query).fetchall()[0][0]:
            print('already exists: ', json_content['nct_id'][0])
            pass

        else:
            print('does not exist: ', json_content['nct_id'][0])
            ## create a new row in the database for this file
            uuid_id = uuid.uuid4()
            query = f"INSERT INTO trials (id, nct_id) VALUES ('{uuid_id}','{json_content['nct_id'][0]}')"
            
            try:
                engine.execute(query)
                print('inserted: ', query)
            except:
                print('error: ', query)
                pass

            # create a new var columnnames_noid that does not include nct_id
            columnnames_noid = columnnames.copy()
            columnnames_noid.remove('nct_id')

            # keep only the keys that are in columnnames_noid
            json_content_slim = {k: v for k, v in json_content.items() if k in columnnames_noid}

            ## loop through the columnnames and get each data point, if it exists, if it does not exist, then set it to None and add it to the database
            for i in json_content_slim:
                print(f'value {i}: ', json_content_slim[i])
                ## remove duplicate values from json_content_slim[i]
                json_content_slim[i] = list(set(json_content_slim[i]))
                query = f"UPDATE trials SET {i} = ARRAY{json_content_slim[i]} WHERE nct_id = '{json_content['nct_id'][0]}'"
                try:
                    engine.execute(query)
                except:
                    print('error: ', query)
                    pass



