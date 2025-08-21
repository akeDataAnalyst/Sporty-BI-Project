
get_ipython().system('pip install mysql-connector-python pandas')

# Path to the generated CSV file
csv_file_path = 'sports_betting_data.csv'

# In your ETL script, after loading the CSV
df = pd.read_csv(csv_file_path)
print(df.dtypes)

import pandas as pd
import mysql.connector
from mysql.connector import Error

# Database connection details
db_config = {
    'host': '',
    'user': '',
    'password': '',
    'database': 'sporty_bi_project'
}

# Path to the generated CSV file
csv_file_path = 'sports_betting_data.csv'

def run_etl():
    try:
        # Step 1: Extract
        print("Starting ETL process...")
        print(f"Reading data from {csv_file_path}...")
        df = pd.read_csv(csv_file_path)
        print(f"Data extracted successfully. Total rows: {len(df)}")

        # Step 2: Transform    
        for col in df.columns:
            if df[col].dtype in ['int64', 'int32']:
                df[col] = df[col].astype(int)
            elif df[col].dtype in ['float64', 'float32']:
                df[col] = df[col].astype(float)
        
        # Ensure timestamp column is correctly formatted
        df['bet_timestamp'] = pd.to_datetime(df['bet_timestamp'])
        
        # Establish the database connection
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Step 3: Load
        print("Connecting to MySQL and loading data...")
        
        # Prepare the SQL INSERT statement
        sql_insert = """
        INSERT INTO bets (
            bet_id, user_id, bet_amount, outcome, sport, bet_type, payout, profit_loss, bet_timestamp
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Convert DataFrame rows into a list of tuples for batch insertion
        records_to_insert = [tuple(row) for row in df.itertuples(index=False, name=None)]
        
        # Execute the batch insert
        cursor.executemany(sql_insert, records_to_insert)
        
        # Commit the changes and close the connection
        conn.commit()
        print(f"Data loaded successfully. {cursor.rowcount} rows inserted.")

    except Error as e:
        print(f"Error during ETL process: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    run_etl()
