# %%
import pandas as pd
from googleapiclient.discovery import build
import googlemaps
import time
import os
import csv
from dotenv import load_dotenv
import googleapiclient.errors

# %%
load_dotenv()
# Load Google API credentials from environment variables
GOOGLE_API_KEY = os.getenv('GOOGLE_API_KEY')
GOOGLE_CSE_ID = os.getenv('GOOGLE_CSE_ID')

# %%
# Initialize the Google Maps client using the provided API key
gmaps = googlemaps.Client(key=GOOGLE_API_KEY)

# %%
# Define filenames for managing the state of the script and logging errors
STATE_FILE = 'script_state.csv'
ERROR_FILE = 'errors.csv'

# %%
def create_search_query(row):
    """
    Generate a search query based on company information.
    Exclude any None or NaN values and concatenate the components with spaces.
    """
    components = [
        row['Company name'],
        row['Country'],
        row['City'],
        row['Province'],
        row['Site address']
    ]
    query = ' '.join(str(item) for item in components if pd.notna(item))
    return query

# %%
def extract_website_url(search_result):
    """
    Retrieve the most probable website URL from the top search results.
    Exclude known aggregator or social media domains.
    """
    if not search_result.get('items'):
        print("No search results found.")
        return None
    
    excluded_domains = [
        'facebook.com', 'linkedin.com', 'twitter.com',
        'instagram.com', 'youtube.com'
    ]
    print("Checking the first five results for a legitimate domain...")
    # Evaluate the first five results for a valid domain
    for index, item in enumerate(search_result['items'][:5]):
        url = item['link'].lower()
        print(f"Checking result {index + 1}: {url}")
        if not any(domain in url for domain in excluded_domains):
            print(f"Valid website found: {item['link']}")
            return item['link']
    
    print("No valid websites found in the top results.")
    return None

# %%
def update_location_info(row):
    """
    Enhance missing location information using the Google Places API.
    Evaluate multiple Places results to identify the most relevant one.
    """
    try:
        # Build a location-based search query (company + city + country).
        search_query = row['Company name']
        print(f"Constructing search query for: {search_query}")
        if pd.notna(row['City']):
            search_query += f", {row['City']}"
            print(f"Added city to search query: {row['City']}")
        if pd.notna(row['Country']):
            search_query += f", {row['Country']}"
            print(f"Added country to search query: {row['Country']}")

        print("Sending request to Google Places API...")
        places_result = gmaps.places(search_query)

        if places_result.get('results'):
            print("Results received from Google Places API.")
            # Select the best place if available, or default to the first.
            # Further refinement could be done by checking user ratings, 
            # proximity, or other heuristics.
            
            # Sort results by rating or select the top if rating is absent
            sorted_results = sorted(
                places_result['results'], 
                key=lambda x: x.get('rating', 0), 
                reverse=True
            )
            best_place = sorted_results[0]
            print(f"Best place found: {best_place['name']} with rating: {best_place.get('rating', 'N/A')}")
            
            # Retrieve place details
            print(f"Fetching details for place ID: {best_place['place_id']}")
            place_details = gmaps.place(best_place['place_id'])['result']
            
            address_components = place_details.get('address_components', [])
            
            location_info = {
                'Country': None,
                'City': None,
                'Province': None,
                'Site address': place_details.get('formatted_address')
            }
            
            for component in address_components:
                types = component['types']
                if 'country' in types:
                    location_info['Country'] = component['long_name']
                    print(f"Country found: {component['long_name']}")
                elif 'locality' in types:
                    location_info['City'] = component['long_name']
                    print(f"City found: {component['long_name']}")
                elif 'administrative_area_level_1' in types:
                    location_info['Province'] = component['long_name']
                    print(f"Province found: {component['long_name']}")
            
            return location_info
            
    except Exception as e:
        print(f"Error updating location info: {str(e)}")
    
    return None

# %%
def save_state(df, filename=STATE_FILE):
    """
    Persist the current state of the data to a CSV file for later resumption.
    """
    print(f"Saving current state to {filename}...")
    df.to_csv(filename, index=False)
    print("State saved successfully.")

# %%
def load_state(filename=STATE_FILE):
    """
    Load a previously saved state file if it exists (i.e., resuming from a prior run).
    """
    print(f"Checking for existing state file: {filename}")
    if os.path.exists(filename):
        print("State file found. Loading...")
        return pd.read_csv(filename)
    print("No state file found. Starting fresh.")
    return None

# %%
def log_error(idx, message, row, filename=ERROR_FILE):
    """
    Record an error row to a separate CSV for debugging or re-processing later.
    """
    print(f"Logging error for index {idx}: {message}")
    with open(filename, 'a', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([idx, message, dict(row)])
    print(f"Error logged for index {idx} successfully.")

# %%
def find_missing_info(df):
    """Identify missing websites and update location information with error logging."""
    # Attempt to resume from a partial state if available
    resumed_df = load_state()
    if resumed_df is not None:
        print("Resuming from saved state...")
        df = resumed_df
    
    # Create Google Search API service
    service = build('customsearch', 'v1', developerKey=GOOGLE_API_KEY)
    
    # Create a copy of rows that still have missing information
    missing_info = df[
        df['Company website'].isna() |
        df['Country'].isna() |
        df['City'].isna() |
        df['Province'].isna() |
        df['Site address'].isna()
    ].copy()
    
    # Counter for daily quota usage
    requests_count = 0
    daily_quota = 1000  # Set to 1000 for free tier with billing enabled
    total_missing = len(missing_info)
    print(f"Total entries with missing information: {total_missing}")

    for idx, row in missing_info.iterrows():
        print(f"Processing row {idx + 1}/{total_missing}...")
        
        # Determine if we still need to fill location or website
        location_missing = (
            pd.isna(row['Country']) or 
            pd.isna(row['City']) or 
            pd.isna(row['Province']) or 
            pd.isna(row['Site address'])
        )
        website_missing = pd.isna(row['Company website'])
        
        try:
            # Update location info
            if location_missing:
                location_info = update_location_info(row)
                if location_info:
                    for field, value in location_info.items():
                        # Update only if we're missing that piece
                        if pd.isna(row[field]) and value:
                            if pd.isna(df.at[idx, field]) and value is not None:
                                # Explicitly convert column to string dtype before setting value
                                if df[field].dtype != 'object':
                                    df[field] = df[field].astype('object')
                                df.at[idx, field] = str(value)
                print(f"Updated location info for row {idx + 1}.")
                # Sleep to be a bit polite to the API
                time.sleep(2)
            
            # Update website
            if website_missing:
                query = create_search_query(df.loc[idx])
                
                try:
                    result = service.cse().list(
                        q=query,
                        cx=GOOGLE_CSE_ID,
                        num=5
                    ).execute()
                except googleapiclient.errors.HttpError as e:
                    if e.resp.status == 429:  # Too Many Requests
                        print("Hit actual API quota limit!")
                        save_state(df)
                        return df
                    else:
                        raise e
                
                website_url = extract_website_url(result)
                if website_url:
                    # Convert column to string dtype before setting value
                    if df['Company website'].dtype != 'object':
                        df['Company website'] = df['Company website'].astype('object')
                    df.at[idx, 'Company website'] = str(website_url)
                    print(f"Updated website for row {idx + 1}.")
                
                requests_count += 1
                if requests_count >= daily_quota:
                    # Instead of sleeping 24 hours, let's store state & exit
                    print(f"Reached daily quota of {daily_quota} requests. Stopping...")
                    save_state(df)
                    return df
                else:
                    # Minimal rate limit
                    time.sleep(1)
            
        except Exception as e:
            print(f"Error processing row {idx}: {str(e)}")
            log_error(idx, str(e), df.loc[idx])
            continue
    
    return df

# %%

def main():
    print("Loading the CSV file...")
    df = pd.read_csv('missing_websites.csv')
    print("CSV file loaded successfully. Finding and updating missing information...")
    
    updated_df = find_missing_info(df)
    print("Missing information updated successfully. Saving final results...")

    output_file = 'companies_info_updated.csv'
    if os.path.exists(output_file):
        timestamp = int(time.time())
        output_file = f'companies_info_updated_{timestamp}.csv'
    
    updated_df.to_csv(output_file, index=False)
    print("Final results saved to 'companies_info_updated.csv'.")
    # Print summary of updates
    print("\nSummary of updates:")
    for column in ['Company website', 'Country', 'City', 'Province', 'Site address']:
        before = len(df[df[column].isna()])
        after = len(updated_df[updated_df[column].isna()])
        print(f"{column}:")
        print(f"  Missing before: {before}")
        print(f"  Missing after: {after}")
        print(f"  Found: {before - after}")
        print()

    # Remove any saved state upon successful completion
    if os.path.exists(STATE_FILE):
        os.remove(STATE_FILE)
        print("Removed temporary state file after successful run.")

# %%
main()

# %%
