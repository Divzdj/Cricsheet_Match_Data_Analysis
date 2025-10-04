import os                                       
import pandas as pd
from sqlalchemy import create_engine, text

# Configuration 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))                                            
DATABASE_PATH = os.path.join(BASE_DIR, 'cricket_data.db')
ENGINE = create_engine(f"sqlite:///{DATABASE_PATH}", echo=False)
QUERIES_FILE_PATH = "analysis_queries.sql"
RESULTS_DIR = os.path.join(BASE_DIR, 'data', 'results', 'summaries')

# Create results folder
os.makedirs(RESULTS_DIR, exist_ok=True)
print(f"Results will be saved to:  {RESULTS_DIR}")
 
#  Utility Functions
def load_queries_from_sql(filepath):
    """Reads queries from a file, separated by '-- name:' tags."""                                                         
    queries = {}#final output dictionary
    current_name = None#holds the name of the SQL query currently being collected
    current_query = []

    try:
        with open(filepath, 'r') as f:
            for line in f:
                if line.strip().startswith('-- name:'):
                    # Save the previous query
                    if current_name and current_query:
                        queries[current_name] = " ".join(current_query).strip()
                    
                    # Start new query
                    current_name = line.split(':')[1].strip()
                    current_query = []
                elif current_name is not None:
                    # Append non-empty lines to the current query
                    stripped_line = line.strip()
                    if stripped_line and not stripped_line.startswith('--'):
                        current_query.append(stripped_line)

            # Save the last query
            if current_name and current_query:
                queries[current_name] = " ".join(current_query).strip()

        return queries
    except FileNotFoundError:
        print(f" Error: Query file '{filepath}' not found at '{filepath}'. Please ensure it is present.")
        return {}
    except Exception as e:
        print(f"Error loading queries from file: {e}")
        return {}

# --- Main Execution ---
def execute_and_export_queries():
    #Loads, executes all 20 SQL queries, and exports results to CSV.
    queries = load_queries_from_sql(QUERIES_FILE_PATH)
    
    if not queries:
        print("Skipping execution as no queries were loaded.")
        return

    print(f"\n--- Executing {len(queries)} Analytical Queries ---")

    try:
        with ENGINE.connect() as conn:
            for name, query in queries.items():
                print(f"-> Executing: {name}")
                
                # Execute query and pull data into Pandas DataFrame
                df = pd.read_sql(text(query), conn)
                
                # Export to CSV
                csv_path = os.path.join(RESULTS_DIR, f"{name}.csv")
                df.to_csv(csv_path, index=False)
                
                print(f"   Saved {len(df)} records to {csv_path}")
                
                # Print preview of the result
                if not df.empty:
                    print(df.head(5).to_markdown(index=False, numalign="left", stralign="left"))
                else:
                    print("   (Result is empty)")

    except Exception as e:
        print(f"\nFATAL DATABASE ERROR during execution: {e}")
        print(f"Please check that your database file exists at {DATABASE_PATH} and the table names are correct.")


if __name__ == "__main__":
    execute_and_export_queries()
