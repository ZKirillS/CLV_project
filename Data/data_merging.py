import pandas as pd
import glob
import dask.dataframe as dd

clients_df = pd.read_csv('new_clients.csv')
transactions_df = pd.read_csv('new_transactions_cat.csv')
amazon_data = pd.read_csv('amazon_prod_cat.csv')

# Merging client data with transactions
merged_clients_transactions = pd.merge(
    transactions_df, clients_df, on='ClientID', how='inner'
)

# Merging transactions data with products data
final_transactions = pd.merge(
    merged_clients_transactions, amazon_data, on='Product', how='inner'
)
final_transactions['Category'] = final_transactions['Category_x']
final_transactions.drop(['Category_x', 'Category_y'], axis=1, inplace=True)
print(final_transactions.columns)
final_transactions.to_csv('final_transactions.csv', index=False, encoding='utf-8')

# Combining reviews data
# Defining folders paths for the review categories
review_folders = {
    'Electronics': 'electronics raw data/*.csv',
    'Grocery': 'grocery raw data/*.csv',
    'Home': 'home raw data/*.csv',
    'Pharma': 'pharma raw data/*.csv',
}

# Processing reviews in batches with Dask
for category, path in review_folders.items():
    review_files = glob.glob(path)
    all_reviews = []

    # Merging reviews in batches for memory efficiency
    for i in range(0, len(review_files), 15):
        batch_files = review_files[i:i+15]
        batch_reviews = dd.read_csv(batch_files)
        batch_reviews['Category'] = category  # Adding the category column
        all_reviews.append(batch_reviews)
    
    # Concatenating all the batches into a single df
    merged_reviews = dd.concat(all_reviews, ignore_index=True)
    
    # Saving the merged file for each category
    merged_reviews.to_csv(f'{category}_merged_reviews.csv', index=False, encoding='utf-8', single_file=True)

# Read merged review files into Dask DataFrames
electronics_reviews = dd.read_csv('Electronics_merged_reviews.csv')
grocery_reviews = dd.read_csv('Grocery_merged_reviews.csv')
home_reviews = dd.read_csv('Home_merged_reviews.csv')
pharma_reviews = dd.read_csv('Pharma_merged_reviews.csv')

# Concatenating all the review data into one large Dask DataFrame
final_reviews = dd.concat([electronics_reviews, grocery_reviews, home_reviews, pharma_reviews], ignore_index=True)

# Merging transactions with reviews using Dask
final_transactions_dd = dd.read_csv('final_transactions.csv')
# complete_data = dd.merge(final_transactions_dd, final_reviews, on='Category', how='left')

# # Saving the final merged dataset
# complete_data.to_csv('final_dataset.csv', index=False, encoding='utf-8', single_file=True)
print("Merging completed.")
