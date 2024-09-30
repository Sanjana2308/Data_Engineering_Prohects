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

# Cleaning user_dim table
def clean_user_dim(user_data):

    user_data.columns = user_data.columns.str.strip().str.lower()

    user_data['user_name'] = user_data['user_name'].str.strip().str.lower()

    user_data['location'] = user_data['location'].str.strip().str.lower().replace('',np.nan)

    user_data['age_group'] = user_data['age_group'].str.strip().str.lower().replace('',np.nan)

    return user_data

# Cleaning content_dim table
def clean_content_dim(content_data):

    content_data.columns = content_data.columns.str.strip().str.lower()

    content_data['title'] = content_data['title'].str.strip().str.lower()

    content_data['genre'] = content_data['genre'].str.strip().str.lower().replace('', np.nan)
    content_data['genre'] = content_data['genre'].fillna("Unknown")

    content_data['release_year'] = pd.to_numeric(content_data['release_year'], errors='coerce')
    content_data['release_year'] = content_data['release_year'].fillna(0)

    return content_data

# Cleaning subscription_plan_dim table
def clean_subscription_plan_dim(subscription_plan_data):

    subscription_plan_data.columns = subscription_plan_data.columns.str.strip().str.lower()

    subscription_plan_data['plan_name'] = subscription_plan_data['plan_name'].str.strip().str.lower()

    subscription_plan_data['price'] = pd.to_numeric(subscription_plan_data['price'], errors='coerce')
    subscription_plan_data = subscription_plan_data.fillna(0)

    subscription_plan_data['features'] = subscription_plan_data['features'].str.strip().str.lower().replace('',np.nan)
    subscription_plan_data['features'] = subscription_plan_data['features'].fillna("Unknown")

    return subscription_plan_data

# Cleaning device_dim table
def clean_device_dim(device_data):

    device_data.columns = device_data.columns.str.strip().str.lower()

    device_data['device_type'] = device_data['device_type'].str.strip().str.lower()

    device_data['operating_system'] = device_data['operating_system'].str.strip().str.lower().replace('',np.nan)
    device_data['operating_system'] = device_data['operating_system'].fillna("Unknown")

    device_data['manufacturer'] = device_data['manufacturer'].str.strip().str.lower().replace('',np.nan)
    device_data['manufacturer'] = device_data['manufacturer'].fillna("Unknown")

    return device_data

# Clean unified_interaction_fact table
def clean_unified_interaction_fact(interaction_data):

    interaction_data.columns = interaction_data.columns.str.strip().str.lower()

    interaction_data['watch_time'] = pd.to_numeric(interaction_data['watch_time'], errors='coerce')
    interaction_data['watch_time'] = interaction_data['watch_time'].fillna(interaction_data['watch_time'].mean())
    interaction_data = interaction_data[interaction_data['watch_time'] >= 0]

    interaction_data['rating'] = pd.to_numeric(interaction_data['rating'], errors='coerce')
    interaction_data['rating'] = interaction_data['rating'].fillna(interaction_data['rating'].median())
    interaction_data = interaction_data[(interaction_data['rating'] >= 1) & (interaction_data['rating'] <= 5)]

    interaction_data['activity_type'] = interaction_data['activity_type'].str.strip().str.lower().replace('',np.nan)
    interaction_data['activity_type'] = interaction_data['activity_type'].fillna("Unknown")

    interaction_data['activity_timestamp'] = pd.to_datetime(interaction_data['activity_timestamp'], errors='coerce')

    interaction_data['interaction_date'] = pd.to_datetime(interaction_data['interaction_date'], errors='coerce')

    interaction_data.dropna()

    return interaction_data

# ------Formatting extracted data------
def formatting_data(user_data, content_data, subscription_plan_data, device_data, interaction_data):
    # Users table
    headers = ["User ID", "User Name", "Location", "Age Group"]
    user_table = tabulate(user_data, headers, tablefmt="grid")
    print("User Table:")
    print(user_table)

    # Content Table
    headers = ["Content ID", "Title", "Genre", "Release Year"]
    content_table = tabulate(content_data, headers, tablefmt="grid")
    print("Content Table:")
    print(content_table)

    # Subscription Plan table
    headers = ["Plan ID", "Plan Name", "Price", "Features"]
    subscription_plan_table = tabulate(subscription_plan_data, headers, tablefmt="grid")
    print("Subscription Plan Table:")
    print(subscription_plan_table)

    # Device Table
    headers = ["Device ID", "Device Type", "Operating System", "Manufacturer"]
    device_table = tabulate(device_data, headers, tablefmt="grid")
    print("Device Table:")
    print(device_table)

    headers = ["Interaction Id", "User ID", "Content ID", "Plan ID", "Device ID", "Watch Time",
               "Rating", "Activity Type", "Activity Timestamp", "Interaction Date"]
    interaction_table = tabulate(interaction_data, headers, tablefmt="grid")
    print("Interaction Table:")
    print(interaction_table)

# Loading data to CSV
def load_data(user_data, content_data, subscription_plan_data, device_data, interaction_data):
    user_data.to_csv('D:/cleaned_files/cleaned_user_data.csv')
    content_data.to_csv('D:/cleaned_files/cleaned_content_data.csv')
    subscription_plan_data.to_csv('D:/cleaned_files/subscription_plan_data.csv')
    device_data.to_csv('D:/cleaned_files/device_data.csv')
    interaction_data.to_csv('D:/cleaned_files/interaction_data.csv')


# Performing ETL
def run_operations():
    engine = sqlalchemy_engine()

    # Extract Data
    user_data, content_data, subscription_plan_data, device_data, interaction_data = extract_data(engine)
    print("Data extracted from SQL file")

    user_data = clean_user_dim(user_data)
    content_data = clean_content_dim(content_data)
    subscription_plan_data = clean_subscription_plan_dim(subscription_plan_data)
    device_data = clean_device_dim(device_data)
    interaction_data = clean_unified_interaction_fact(interaction_data)
    print("Data Transformed")

    # Formatting data in table forms
    formatting_data(user_data, content_data, subscription_plan_data, device_data, interaction_data)

    # Loading data
    load_data(user_data, content_data, subscription_plan_data, device_data, interaction_data)
    print("Data Loaded into CSV Files")


if __name__ == "__main__":
    print("*" * 65)
    print("🙂🙂 Welcome to Data Processing and Recommendation Engine 🙂🙂")
    print("*"*65)
    run_operations()







