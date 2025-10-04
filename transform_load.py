import os#for file path manipulations
import json#for reading files
import pandas as pd
from sqlalchemy import create_engine#for database connection.

#  SETUP 
# Base directories 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
RAW_JSON_DIR = os.path.join(BASE_DIR, 'data', 'raw_json')
DATABASE_PATH = os.path.join(BASE_DIR, 'cricket_data.db')

# SQLAlchemy engine for SQLite database

ENGINE = create_engine(f'sqlite:///{DATABASE_PATH}')#This object manages the connection to the SQLite database file

# TRANSFORMATION FUNCTIONS 

def parse_match_metadata(match_data, match_format):
    #Extracts high-level match metadata for the 'Matches' table.
    
    info = match_data.get('info', {})
    
    # Generate a unique Match ID (filename)
    match_id = info.get('match_data_version', 'v0') + '_' + info.get('dates', ['1970-01-01'])[0] + '_' + info.get('event', {}).get('name', 'UnknownEvent')
    
    # If the unique ID is just a date, try to make it more unique with teams
    if match_id.count('_') < 3 and info.get('teams'):
        match_id += '_' + '_vs_'.join(info['teams'])
    
    # Clean and simplify the ID
    match_id = match_id.replace(' ', '_').replace('/', '_').replace('.', '').replace('-', '_')

    # Extract required fields, using .get() for safety
    matches_df = pd.DataFrame([{
        'match_id': match_id,
        'format': match_format,
        'season': info.get('dates', ['1970'])[0][:4],
        'date': info.get('dates', ['1970-01-01'])[0],
        'venue': info.get('venue', 'Unknown Venue'),
        'gender': info.get('gender', 'male'),
        'teams_str': ', '.join(info.get('teams', [])),
        'toss_winner': info.get('toss', {}).get('winner', None),
        'toss_decision': info.get('toss', {}).get('decision', None),
        'winner': info.get('outcome', {}).get('winner', 'No Result'),
        'by_runs': info.get('outcome', {}).get('by', {}).get('runs', 0),
        'by_wickets': info.get('outcome', {}).get('by', {}).get('wickets', 0),
        'player_of_match': ', '.join(info.get('player_of_match', [])),
    }])
    
    return matches_df.drop_duplicates(subset=['match_id']), match_id


def parse_deliveries(match_data, match_id):
    #Extracts ball-by-ball delivery data for the 'Deliveries' table.
    all_deliveries = []
    
    for inning in match_data.get('innings', []):
        team = inning.get('team', None)
        inning_number = inning.get('innings_number', None)
        
        # Iterate through every delivery (ball) in the inning
        for over_data in inning.get('overs', []):
            over_number = over_data.get('over', None)
            
            for delivery in over_data.get('deliveries', []):
                
                # Create a unique delivery ID (MatchID_Inning_Over.Ball)
                delivery_id = f"{match_id}_{inning_number}_{over_number}.{delivery.get('ball', 0)}"

                # Extract the complex nested data
                wickets = delivery.get('wickets', [])
                
                # Extract first wicket details if any
                is_wicket = 1 if wickets else 0
                wicket_kind = wickets[0].get('kind', None) if wickets else None
                player_out = wickets[0].get('player_out', None) if wickets else None
                
                # Delivery-level details
                all_deliveries.append({
                    'delivery_id': delivery_id,
                    'match_id': match_id,
                    'inning': inning_number,
                    'batting_team': team,
                    'bowling_team': delivery.get('bowler_team', None),
                    'over': over_number,
                    'ball': delivery.get('ball', 0),
                    'batter': delivery.get('batter', None),
                    'bowler': delivery.get('bowler', None),
                    'runs_batter': delivery.get('runs', {}).get('batter', 0),
                    'runs_extras': delivery.get('runs', {}).get('extras', 0),
                    'runs_total': delivery.get('runs', {}).get('total', 0),
                    'extras_type': list(delivery.get('extras', {}).keys())[0] if delivery.get('extras') else None,
                    'is_wicket': is_wicket,
                    'wicket_kind': wicket_kind,
                    'player_out': player_out,
                })

    return pd.DataFrame(all_deliveries)


def process_and_load_data():
    # Main ETL function, now modified to process JSON files and load data into separate, format-specific tables (e.g., test_matches, odi_deliveries).
    all_matches_df = []
    all_deliveries_df = []
    
    # Dictionaries to store lists of DataFrames, keyed by format (test, odi, t20, ipl)
    all_matches_by_format = {}
    all_deliveries_by_format = {}
    
    print(f"--- Starting Transformation & Loading to {DATABASE_PATH} ---")

    # Iterate through each format folder (test, odi, t20, ipl)
    for match_format in os.listdir(RAW_JSON_DIR):
        format_dir = os.path.join(RAW_JSON_DIR, match_format)
        
        if not os.path.isdir(format_dir):
            continue

        # Initialize lists for the current format
        all_matches_by_format[match_format] = []
        all_deliveries_by_format[match_format] = []

        print(f"\nProcessing {match_format.upper()} matches from: {format_dir}")
        
        json_files = [f for f in os.listdir(format_dir) if f.endswith('.json')]
        
        for filename in json_files:
            file_path = os.path.join(format_dir, filename)
            
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    match_data = json.load(f)
                
                # Transforms the data
                matches_df_single, match_id = parse_match_metadata(match_data, match_format)
                deliveries_df_single = parse_deliveries(match_data, match_id)
                
                # Append to the format-specific list
                all_matches_by_format[match_format].append(matches_df_single)
                all_deliveries_by_format[match_format].append(deliveries_df_single)
                
            except json.JSONDecodeError:
                print(f"   WARNING: Failed to parse JSON in {filename}. Skipping.")
            except Exception as e:
                print(f"   ERROR processing {filename}: {e}. Skipping.")

        print(f"  -> Successfully parsed {len(json_files)} files for {match_format.upper()}.")


    #  Database Loading (Separate Tables) 
    
    print("\n--- Final Data Loading (Separate Tables) ---")
    
    total_matches_loaded = 0
    total_deliveries_loaded = 0

    try:
        # Loop through the collected data by format
        for match_format in all_matches_by_format.keys():
            
            # Skip if no files were found for this format
            if not all_matches_by_format[match_format]:
                continue
                
            #  Concatenate all match DataFrames for the current format
            final_matches_df = pd.concat(all_matches_by_format[match_format], ignore_index=True)
            match_table_name = f"{match_format}_matches"
            
            # Load Matches Table
            print(f"Inserting {len(final_matches_df):,} records into '{match_table_name}'...")
            final_matches_df.to_sql(match_table_name, con=ENGINE, if_exists='replace', index=False)
            total_matches_loaded += len(final_matches_df)

            #  Concatenate all delivery DataFrames for the current format
            final_deliveries_df = pd.concat(all_deliveries_by_format[match_format], ignore_index=True)
            delivery_table_name = f"{match_format}_deliveries"
            
            # Load Deliveries Table
            print(f"Inserting {len(final_deliveries_df):,} records into '{delivery_table_name}'...")
            final_deliveries_df.to_sql(delivery_table_name, con=ENGINE, if_exists='replace', index=False, chunksize=10000)
            total_deliveries_loaded += len(final_deliveries_df)
        
        print(f"\nTotal Records Loaded: {total_matches_loaded:,} Matches, {total_deliveries_loaded:,} Deliveries.")
        
    except Exception as e:
        print(f"FATAL DATABASE ERROR during loading: {e}")
        return

    print(f"\nETL Phase Complete! Database available at: {DATABASE_PATH}")


if __name__ == "__main__":
    process_and_load_data()
