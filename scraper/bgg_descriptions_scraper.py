import requests
import pandas as pd
import csv
import os
import json
import time

def get_game_description(game_id):
    """Fetch game description using the BGG API"""
    try:
        # Use the BGG API to get game details
        url = f"https://boardgamegeek.com/xmlapi2/thing?id={game_id}&stats=1"
        response = requests.get(url)
        
        # Check if the request was successful
        if response.status_code == 200:
            # The API returns XML, we can use the built-in XML parser
            from xml.etree import ElementTree as ET
            import html
            
            root = ET.fromstring(response.content)
            
            # Find the description element
            description_element = root.find(".//description")
            if description_element is not None and description_element.text:
                # Clean up the description - decode HTML entities
                description = html.unescape(description_element.text.strip())
                
                # Replace remaining problematic characters
                description = description.replace('\n', ' ').replace('\r', '')
                
                return description
            else:
                return "No description available"
        elif response.status_code == 429:
            # Too many requests, wait and try again
            time.sleep(5)
            return get_game_description(game_id)
        else:
            return f"Error: API returned status code {response.status_code}"
    
    except Exception as e:
        return f"Error: {str(e)}"

def main():
    # Start timing the execution
    start_time = time.time()
    
    # Read the CSV file
    df = pd.read_csv('boardgames_ranks.csv')
                                                                                                                                                                                           
    # Limit to first 30000 games
    df = df.head(30000)
    
    # Check for checkpoint file
    checkpoint_file = 'descriptions_scraping_checkpoint.json'
    start_index = 0
    
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
                start_index = checkpoint_data.get('last_index', 0) + 1
                print(f"Resuming from game #{start_index}")
        except json.JSONDecodeError:
            # Handle corrupted checkpoint file
            print("Checkpoint file corrupted. Starting from the beginning.")
            start_index = 0
    
    # Create or append to output file
    file_mode = 'a' if start_index > 0 else 'w'
    
    try:
        with open('game_descriptions.csv', file_mode, newline='', encoding='utf-8') as file:
            writer = csv.writer(file)
            
            # Write header only if starting fresh
            if start_index == 0:
                writer.writerow(['id', 'name', 'description'])
            
            # Process games from checkpoint
            for index, row in df.iloc[start_index:].iterrows():
                try:
                    description = get_game_description(row['id'])
                    writer.writerow([row['id'], row['name'], description])
                    
                    # Save checkpoint after each game
                    with open(checkpoint_file, 'w') as f:
                        json.dump({'last_index': index}, f)
                    
                    # Flush the CSV file to ensure data is written
                    file.flush()
                    
                    # Print progress every 20 games
                    if (index - start_index + 1) % 20 == 0:
                        print(f"Processed {index-start_index+1}/{len(df)-start_index} games")
                    
                    # More conservative delay - 0.3 seconds between requests
                    time.sleep(0.3)
                        
                except Exception as e:
                    print(f"Error processing game {row['id']}: {str(e)}")
                    # Continue to next game
    
    except KeyboardInterrupt:
        # Calculate elapsed time even if interrupted
        elapsed_time = time.time() - start_time
        print(f"\nScript interrupted by user. Progress saved at checkpoint.")
        print(f"Total time elapsed: {elapsed_time:.2f} seconds")
    except Exception as e:
        # Calculate elapsed time even if error occurs
        elapsed_time = time.time() - start_time
        print(f"Unexpected error: {str(e)}")
        print(f"Total time elapsed: {elapsed_time:.2f} seconds")
    
    # Calculate and print total execution time
    elapsed_time = time.time() - start_time
    print(f"Total time elapsed: {elapsed_time:.2f} seconds")
    print("To resume, simply run the script again.")
    print("Progress saved at last checkpoint.")
    
    print("API scraping completed or interrupted. Last checkpoint saved.")

if __name__ == "__main__":
    main()