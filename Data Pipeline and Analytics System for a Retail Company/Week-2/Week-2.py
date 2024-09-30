import numpy as np
from sqlalchemy import create_engine
import pandas as pd

def sqlalchemy_engine():
    server = 'DESKTOP-TOQNTIJ'
    database = 'Retail_Project_Cleaned_DB'
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver}&trusted_connection=yes"
    )

    engine = create_engine(connection_string)
    return engine

# Extracting data from CSV
def extract_data():
    customer_data = pd.read_csv('D:\\Data_Engineering_Projects\\Data Pipeline and Analytics System for a Retail Company\\Week-2\\customer_data.csv')
    product_data = pd.read_csv('D:\\Data_Engineering_Projects\\Data Pipeline and Analytics System for a Retail Company\\Week-2\\product_data.csv')
    store_data = pd.read_csv('D:\\Data_Engineering_Projects\\Data Pipeline and Analytics System for a Retail Company\\Week-2\\store_data.csv')
    date_data = pd.read_csv('D:\\Data_Engineering_Projects\\Data Pipeline and Analytics System for a Retail Company\\Week-2\\date_data.csv')
    sales_data = pd.read_csv('D:\\Data_Engineering_Projects\\Data Pipeline and Analytics System for a Retail Company\\Week-2\\sales_data.csv')

    return customer_data, product_data, store_data, date_data, sales_data

# Clean customer_dim data
def clean_customer_data(customer_data):

    customer_data.columns = customer_data.columns.str.strip().str.lower()

    customer_data['customer_name'] = customer_data['customer_name'].str.strip().str.lower()

    customer_data['email'] = customer_data['email'].str.strip().replace('',np.nan)
    customer_data['email'] = customer_data['email'].fillna("Unknown")

    customer_data['location'] = customer_data['location'].str.strip().str.lower().replace('',np.nan)
    customer_data['location'] = customer_data['location'].fillna("Unknown")

    customer_data['registration_date'] = pd.to_datetime(customer_data['registration_date'], errors='coerce')
    customer_data['registration_date'] = customer_data['registration_date'].fillna(pd.Timestamp('2024-10-01'))

    return customer_data

# Clean product_dim data
def clean_product_data(product_data):

    product_data.columns = product_data.columns.str.strip().str.lower()

    product_data['product_name'] = product_data['product_name'].str.strip().str.lower()

    product_data['category'] = product_data['category'].str.strip().str.lower().replace('',np.nan)
    product_data['category'] = product_data['category'].fillna("Unknown")

    product_data['brand'] = product_data['brand'].str.strip().str.lower().replace('',np.nan)
    product_data['brand'] = product_data['brand'].fillna("Unknown")

    product_data['price'] = pd.to_numeric(product_data['price'], errors='coerce')
    product_data['price'] = product_data['price'].fillna(product_data['price'].mean())

    return product_data

# Clean store_dim data
def clean_store_data(store_data):

    store_data.columns = store_data.columns.str.strip().str.lower()

    store_data['store_name'] = store_data['store_name'].str.strip().str.lower()

    store_data['address'] = store_data['address'].str.strip().str.lower().replace('',np.nan)
    store_data['address'] = store_data['address'].fillna("Unknown")

    store_data['city'] = store_data['city'].str.strip().str.lower().replace('',np.nan)
    store_data['city'] = store_data['city'].fillna("Unknown")

    store_data['state'] = store_data['state'].str.strip().str.lower().replace('',np.nan)
    store_data['state'] = store_data['state'].fillna("Unknown")

    store_data['country'] = store_data['country'].str.strip().str.lower().replace('',np.nan)
    store_data['country'] = store_data['country'].fillna("Unknown")

    return store_data

# Clean date_dim data
def clean_date_data(date_data):

    date_data.columns = date_data.columns.str.strip().str.lower()

    date_data['full_date'] = pd.to_datetime(date_data['full_date'], errors='coerce')
    date_data.dropna()

    date_data['day_of_week'] = date_data['day_of_week'].str.strip().str.lower().replace('',np.nan)
    date_data['day_of_week'] = date_data['day_of_week'].fillna("Unknown")

    date_data['month'] = date_data['month'].str.strip().str.lower().replace('',np.nan)
    date_data['month'] = date_data['month'].fillna("Unknown")

    date_data['year'] = pd.to_numeric(date_data['year'], errors='coerce')
    date_data['year'] = date_data['year'].fillna(2024)

    date_data['quarter'] = date_data['quarter'].str.strip().str.lower().replace('',np.nan)
    date_data['quarter'] = date_data['quarter'].fillna("Q3")

    return date_data

# Clean sales_fact data
def clean_sales_data(sales_data):

    sales_data.columns = sales_data.columns.str.strip().str.lower()

    sales_data['quantity'] = pd.to_numeric(sales_data['quantity'], errors='coerce')
    sales_data['quantity'] = sales_data['quantity'].fillna(0)

    sales_data['sales_amount'] = pd.to_numeric(sales_data['sales_amount'], errors='coerce')
    sales_data['sales_amount'] = sales_data['sales_amount'].fillna(sales_data['sales_amount'].mean())

    sales_data['discount'] = pd.to_numeric(sales_data['discount'], errors='coerce')
    sales_data['discount'] = sales_data['discount'].fillna(sales_data['discount'].median())

    sales_data['order_date'] = pd.to_datetime(sales_data['order_date'], errors='coerce')
    sales_data['order_date'] = sales_data['order_date'].fillna(pd.Timestamp('2024-10-01'))

    return sales_data

# Saving cleaned_data
def save_cleaned(customer_data, product_data, store_data, date_data, sales_data):
    customer_data.to_csv('D:\\cleaned_files\\cleaned_customer_data.csv')
    product_data.to_csv('D:\\cleaned_files\\cleaned_product_data.csv')
    store_data.to_csv('D:\\cleaned_files\\cleaned_store_data.csv')
    date_data.to_csv('D:\\cleaned_files\\cleaned_date_data.csv')
    sales_data.to_csv('D:\\cleaned_files\\cleaned_sales_data.csv')


# Loading data to SQL
def load_data(customer_data, product_data, store_data, date_data, sales_data):
    engine = sqlalchemy_engine()

    customer_data.to_sql('customer_dim', con=engine, if_exists='append', index=False)
    product_data.to_sql('product_dim', con=engine, if_exists='append', index=False)
    store_data.to_sql('store_dim', con=engine, if_exists='append', index=False)
    date_data.to_sql('date_dim', con=engine, if_exists='append', index=False)
    sales_data.to_sql('sales_dim', con=engine, if_exists='append', index=False)

# Perform ETL
def run_operations():

    print("Extracting data from CSV...")
    customer_data, product_data, store_data, date_data, sales_data = extract_data()

    print("Transforming data...")
    customer_data = clean_customer_data(customer_data)
    product_data = clean_product_data(product_data)
    store_data = clean_store_data(store_data)
    date_data = clean_date_data(date_data)
    sales_data = clean_sales_data(sales_data)

    print("Transformed Data:")
    print(customer_data)
    print(product_data)
    print(store_data)
    print(date_data)
    print(sales_data)

    print("Saving cleaned data to CSV")
    save_cleaned(customer_data, product_data, store_data, date_data, sales_data)

    print("Loading data to SQL...")
    load_data(customer_data, product_data, store_data, date_data, sales_data)



if __name__=='__main__':
    run_operations()
