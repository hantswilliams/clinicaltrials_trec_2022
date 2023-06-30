import os
from sqlalchemy import create_engine, text
import dotenv

dotenv.load_dotenv()

engine = create_engine(os.environ["cockroachdb_url"])
conn = engine.connect()

res = conn.execute(text("SELECT now()")).fetchall()
print(res)


## delete table Terms and DocumentTerm
conn.execute(text("DROP TABLE DocumentTerm"))
conn.execute(text("DROP TABLE Terms"))


termsTableCreate = '''CREATE TABLE IF NOT EXISTS Terms (
                    TermID SERIAL PRIMARY KEY,
                    Term TEXT
                )'''

resetSequence = '''SELECT setval(pg_get_serial_sequence('Terms', 'TermID'), 1, false);'''

documentTermTableCreate = '''CREATE TABLE IF NOT EXISTS DocumentTerm (
                    DocumentID TEXT,
                    TermID INTEGER,
                    Count INTEGER,
                    FOREIGN KEY (TermID) REFERENCES Terms (TermID)
                )'''

conn.execute(text(termsTableCreate))
conn.execute(text(documentTermTableCreate))
conn.execute(text(resetSequence))


## get first 10 rows from table Terms
res = conn.execute(text("SELECT * FROM Terms LIMIT 100")).fetchall()
print(res)

## get first 10 rows from table DocumentTerm
res = conn.execute(text("SELECT * FROM DocumentTerm LIMIT 100")).fetchall()
print(res)
