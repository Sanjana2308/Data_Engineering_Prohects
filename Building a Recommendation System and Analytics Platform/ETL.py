import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from tabulate import tabulate



# ------Establishing connection with SQL Database------
def sqlalchemy_engine():
    server = 'DESKTOP-TOQNTIJ'
    database = 'Streaming_temp2'
    driver = 'ODBC Driver 17 for SQL Server'

    connection_string = (
        f"mssql+pyodbc://@{server}/{database}"
        f"?driver={driver}&trusted_connection=yes"
    )

    engine = create_engine(connection_string)
    print("🎉🎉 Connection established to SQL Server using SQLAlchemy 🎉🎉")
    print("*" * 65)
    return engine


# ------For extracting and printing data from SQL Database------
def extract_data(engine):
    # For user_dim table
    user_query = "SELECT * FROM user_dim;"
    user_data = pd.read_sql(user_query, con=engine)


    # For content_dim table
    content_query = "SELECT * FROM content_dim;"
    content_data = pd.read_sql(content_query, con=engine)


    # For subscription_plan_dim table
    subscription_plan_query = "SELECT * FROM subscription_plan_dim"
    subscription_plan_data = pd.read_sql(subscription_plan_query, con=engine)

    # For device_dim table
    device_query = "SELECT * FROM device_dim;"
    device_data = pd.read_sql(device_query, con=engine)

    unified_interaction_query = "SELECT * FROM unified_interaction_fact;"
    interaction_data = pd.read_sql(unified_interaction_query, con=engine)

    return user_data, content_data, subscription_plan_data, device_data, interaction_data

# ------Formatting extracted data------
def formatting_extracted_data(user_data, content_data, subscription_plan_data, device_data, interaction_data):
    # Users table
    headers = ["User ID", "User Name", "Location", "Age Group"]
    user_table = tabulate(user_data, headers, tablefmt="grid")
    print("-" * 16)
    print("✨User Table: 👇🏻")
    print("-" * 16)
    print(user_table)

    # Content Table
    headers = ["Content ID", "Title", "Genre", "Release Year"]
    content_table = tabulate(content_data, headers, tablefmt="grid")
    print("-" * 18)
    print("✨Content Table:👇🏻")
    print("-" * 18)
    print(content_table)

    # Subscription Plan table
    headers = ["Plan ID", "Plan Name", "Price", "Features"]
    subscription_plan_table = tabulate(subscription_plan_data, headers, tablefmt="grid")
    print("-" * 28)
    print("✨Subscription Plan Table:👇🏻")
    print("-" * 28)
    print(subscription_plan_table)

    # Device Table
    headers = ["Device ID", "Device Type", "Operating System", "Manufacturer"]
    device_table = tabulate(device_data, headers, tablefmt="grid")
    print("-" * 18)
    print("✨Device Table:👇🏻")
    print("-" * 18)
    print(device_table)

    headers = ["Interaction Id", "User ID", "Content ID", "Plan ID", "Device ID", "Watch Time",
               "Rating", "Activity Type", "Activity Timestamp", "Interaction Date"]
    interaction_table = tabulate(interaction_data, headers, tablefmt="grid")
    print("-" * 23)
    print("✨Interaction Table: 👇🏻")
    print("-" * 23)
    print(interaction_table)


def transform_data(user_data, content_data, subscription_plan_data, device_data, interaction_data):

    # User Data cleaning
    user_data.columns = user_data.columns.str.strip()

    user_data["user_name"] = user_data["user_name"].astype(str).str.strip()
    user_data["location"] = user_data["location"].astype(str).str.strip().replace('', np.nan)
    user_data["location"] = user_data["location"].fillna('Unknown')

    user_data["age_group"] = user_data["age_group"].astype(str).str.strip()

    # Content Data
    content_data.columns = content_data.columns.str.strip()

    content_data["title"] = content_data["title"].astype(str).str.strip()
    content_data["genre"] = content_data["genre"].astype(str).str.strip().replace('', np.nan)
    content_data["release_year"] = content_data["release_year"].astype(str).str.strip().replace('', np.nan)

    content_data["genre"] = content_data["genre"].fillna('Unknown')
    content_data["release_year"] = content_data["release_year"].fillna('Unknown')

    # Subscription Plan Data
    subscription_plan_data.columns = subscription_plan_data.columns.str.strip()

    subscription_plan_data["plan_name"] = subscription_plan_data["plan_name"].astype(str).str.strip()
    subscription_plan_data["features"] = subscription_plan_data["features"].astype(str).str.strip().replace('', np.nan)
    subscription_plan_data["features"] = subscription_plan_data["features"].fillna('Unknown')

    # Ensure 'price' is numeric and fill missing values with the median price
    subscription_plan_data["price"] = pd.to_numeric(subscription_plan_data["price"], errors='coerce')
    subscription_plan_data["price"] = subscription_plan_data["price"].fillna(subscription_plan_data["price"].median())

    device_data.columns = device_data.columns.str.strip()

    device_data["device_type"] = device_data["device_type"].astype(str).str.strip()
    device_data["operating_system"] = device_data["operating_system"].astype(str).str.strip().replace('', np.nan)
    device_data["manufacturer"] = device_data["manufacturer"].astype(str).str.strip().replace('', np.nan)

    device_data["operating_system"] = device_data["operating_system"].fillna('Unknown')
    device_data["manufacturer"] = device_data["manufacturer"].fillna('Unknown')

    interaction_data.columns = interaction_data.columns.str.strip()

    interaction_data["interaction_date"] = pd.to_datetime(interaction_data["interaction_date"].astype(str).str.strip(), errors='coerce')
    interaction_data["interaction_date"] = interaction_data["interaction_date"].fillna(pd.Timestamp('2024-01-01'))

    interaction_data["watch_time"] = pd.to_numeric(interaction_data["watch_time"], errors='coerce')
    interaction_data["watch_time"] = interaction_data["watch_time"].fillna(0)

    interaction_data["rating"] = pd.to_numeric(interaction_data["rating"], errors='coerce')
    interaction_data["rating"] = interaction_data["rating"].fillna(0)

    interaction_data["activity_type"] = pd.to_numeric(interaction_data["activity_type"], errors='coerce')
    interaction_data["activity_type"] = interaction_data["activity_type"].fillna(0)

    user_data.to_csv('../data/cleaned/cleaned_customer_data.csv', index=False)
    content_data.to_csv('../data/cleaned/cleaned_product_data.csv', index=False)
    subscription_plan_data.to_csv('../data/cleaned/cleaned_store_data.csv', index=False)
    device_data.to_csv('../data/cleaned/cleaned_date_data.csv', index=False)
    interaction_data.to_csv('../data/cleaned/cleaned_sales_data.csv', index=False)

    return user_data, content_data, subscription_plan_data, device_data, interaction_data

def truncate_table(engine, table_name):
    with engine.connect() as connection:
        truncate_query = text(f"TRUNCATE TABLE {table_name};")
        connection.execute(truncate_query)
        print(f"Table {table_name} truncated.")

def load_data_to_sql(user_data, content_data, subscription_plan_data, device_data, interaction_data):
    engine = sqlalchemy_engine()

    # truncate_table(engine, "unified-interaction-fact")
    # truncate_table(engine, "device_dim")
    # truncate_table(engine, "subscription_plan_dim")
    # truncate_table(engine, "content_dim")
    # truncate_table(engine, "user_dim")

    user_data.to_sql('user_dim', con=engine, if_exists='append', index=False)
    content_data.to_sql('content_dim', con=engine, if_exists='append', index=False)
    subscription_plan_data.to_sql('subscription_plan_dim', con=engine, if_exists='append', index=False)
    device_data.to_sql('device_dim', con=engine, if_exists='append', index=False)
    interaction_data.to_sql('unified_interaction_fact', con=engine, if_exists='append', index=False)
    print("Data loaded into SQL")


def run_operations():
    engine = sqlalchemy_engine()

    # Extract Data
    user_data, content_data, subscription_plan_data, device_data, interaction_data = extract_data(engine)
    formatting_extracted_data(user_data, content_data, subscription_plan_data, device_data, interaction_data)

    # Transform Data
    user_data, content_data, subscription_plan_data, device_data, interaction_data= transform_data(user_data, content_data, subscription_plan_data, device_data, interaction_data)

    #load_data
    load_data_to_sql(user_data, content_data, subscription_plan_data, device_data, interaction_data)





if __name__ == "__main__":
    print("*" * 65)
    print("🙂🙂 Welcome to Data Processing and Recommendation Engine 🙂🙂")
    print("*"*65)
    run_operations()







