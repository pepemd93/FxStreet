import requests
import duckdb
import unittest
import io
import pyarrow.parquet as pq
import subprocess
from flask import Flask,send_file

app = Flask(__name__)

def load_data():
    url = "https://sde-test-data-sltezl542q-ew.a.run.app/"

    # Make a GET request to the URL
    response = requests.get(url)

    # Check if the request was successful
    if response.status_code == 200:
        file_obj = io.BytesIO(response.content)
        data= pq.read_table(file_obj)
        
        # Create a DuckDB connection
        con = duckdb.connect(database='results/mydatabase.duckdb', read_only=False)
        
        # Create a table in DuckDB
        con.execute(''' CREATE TABLE IF NOT EXISTS funnel_data (
            event_timestamp BIGINT, 
            event_name VARCHAR, 
            event_params STRUCT(key VARCHAR, value STRUCT(int_value BIGINT, string_value VARCHAR))[], 
            user_id VARCHAR, 
            user_pseudo_id VARCHAR, 
            session_id BIGINT
            ) ''')
        
        # Insert the data into the table
        con.execute("INSERT INTO funnel_data SELECT * FROM data")
        
        # Commit the changes and close the connection
        con.commit()
        con.close()
        print("Data loaded successfully.")
    else:
        print("Failed to retrieve data from the URL.")
    
    return len(data)

def run_dbt():
    # Define the command to run dbt
    command = ['dbt', 'run']

    # Run the dbt command
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    # Check the output
    if process.returncode == 0:
        print("dbt run completed successfully.")
    else:
        print(f"dbt run failed with error: {stderr.decode('utf-8')}")

@app.route('/', methods=['GET'])
def handle_get_request():
    # Call the load_data function
    n_rows = load_data()

    # Connect to the DuckDB database
    con = duckdb.connect(database='results/mydatabase.duckdb', read_only=True)

    # Check if the table exists
    result = con.execute("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='funnel_data'")
    assert result.fetchone()[0] == 1

    # Check the number of rows in the table
    result = con.execute("SELECT COUNT(*) FROM funnel_data")
    assert result.fetchone()[0] >= n_rows  # At least the same amount as inserted

    # Close the connection
    con.close()

    # Run the transformation via dbt
    run_dbt()

    return send_file('results/mydatabase.duckdb',mimetype='application/octet-stream')

if __name__ == '__main__':
    app.run()