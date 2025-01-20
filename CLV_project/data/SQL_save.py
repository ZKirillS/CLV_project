from sqlalchemy import create_engine
import pandas as pd
from config import DATABASE

engine = create_engine(
    f"postgresql://{DATABASE['user']}:{DATABASE['password']}@{DATABASE['host']}:{DATABASE['port']}/{DATABASE['dbname']}"
)
conn = engine.connect()

reviews = pd.read_csv('clean_reviews.csv')
transactions = pd.read_csv('clean_transactions.csv')
rfm = pd.read_csv('clean_rfm.csv')

reviews.to_sql('clients_table', conn, if_exists='replace', index=False)
transactions.to_sql('transactions_table', conn, if_exists='replace', index=False)
rfm.to_sql('metrics_table', conn, if_exists='replace', index=False)

engine.dispose()
