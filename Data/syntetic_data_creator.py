import pandas as pd
import numpy as np
from faker import Faker
from random import choice, randint, uniform

fake = Faker()

amazon_data = pd.read_csv('C://Users//chekf//OneDrive//Документы//Vis DI-BOOTCAMP//Final Project//Data_amazon_products.csv')
category_group = amazon_data.groupby('Category')['title'].unique().to_dict()

payment_methods = ['Credit Card','Cash','Debit Card','PayPal']
countries = ['USA','Canada','UK']

num_clients = 2500
max_transaction_per_client = 15

def generate_client(num_clients):
    clients = []
    for _ in range(num_clients):
        client_id = fake.uuid4()
        name = fake.name()
        age = randint(18,70)
        gender = choice(['Male','Female'])
        country = choice(countries)
        loyalty = choice(['New','Vip','Returning'])

        clients.append({
            'Client_id': client_id,
            'Name': name,
            'Age': age,
            'Gender': gender,
            'Country': country,
            'Loyalty': loyalty
        })
    return pd.DataFrame(clients)


def generate_transactions(client_df, categories, max_transaction_per_client):
    transactions = []
    for _, client in client_df.iterrows():
        num_transactions = randint(1, max_transaction_per_client)
        for _ in range(num_transactions):
            transaction_id = fake.uuid4()
            client_id = client['Client_id']
            category = choice(list(categories.keys()))
            product = choice(categories[category])
            price = round(uniform(10, 500), 2)
            quantity = randint(1, 5)
            total = round(price * quantity, 2)
            transaction_date = fake.date_between(start_date='-2y', end_date='today')
            payment_method = choice(payment_methods)

            transactions.append({
                'TransactionID': transaction_id,
                'ClientID': client_id,
                'Category': category,
                'Product': product,
                'Price': price,
                'Quantity': quantity,
                'Total': total,
                'TransactionDate': transaction_date,
                'PaymentMethod': payment_method
            })
    return pd.DataFrame(transactions)

print('Genrating clients data...')
clients_df = generate_client(num_clients)
print('Generating transactions data...')
transactions_df = generate_transactions(clients_df, category_group, max_transaction_per_client)

clients_df.to_csv('new_clients.csv', index=False)
transactions_df.to_csv('new_transactions.csv', index=False)

print('Data sucsessfuly generated')