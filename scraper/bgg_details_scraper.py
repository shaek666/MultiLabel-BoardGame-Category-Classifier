import requests
import pandas as pd
import csv
import os
import json
import time
from xml.etree import ElementTree as ET
import concurrent.futures
from tqdm import tqdm

def get_game_details(game_id):
    """Fetch detailed game information using the BGG API"""
    try:
        # Reduce delay to 0.7 seconds - balanced approach
        time.sleep(0.7)
        
        # Use the BGG API to get game details
        url = f"https://boardgamegeek.com/xmlapi2/thing?id={game_id}&stats=1"
        response = requests.get(url, timeout=10)
        
        # Check if the request was successful
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            
            # Initialize a dictionary to store all details
            details = {
                'id': game_id,
                'type': '',
                'category': [],
                'mechanism': [],
                'designer': [],
                'artist': [],
                'publisher': [],
                'min_players': '',
                'max_players': '',
                'playing_time': '',
                'min_age': '',
                'complexity_rating': '',
                'integrates_with': []
            }
            
            # Get the item element (the game)
            item = root.find('.//item')
            if item is not None:
                # Get type
                details['type'] = item.get('type', '')
                
                # Get categories, mechanisms, designers, artists, publishers
                for link in item.findall('.//link'):
                    link_type = link.get('type', '')
                    link_value = link.get('value', '')
                    
                    if link_type == 'boardgamecategory':
                        details['category'].append(link_value)
                    elif link_type == 'boardgamemechanic':
                        details['mechanism'].append(link_value)
                    elif link_type == 'boardgamedesigner':
                        details['designer'].append(link_value)
                    elif link_type == 'boardgameartist':
                        details['artist'].append(link_value)
                    elif link_type == 'boardgamepublisher':
                        details['publisher'].append(link_value)
                    elif link_type == 'boardgameintegration':
                        details['integrates_with'].append(link_value)
                
                # Get player count, playing time, and age range
                min_players = item.find('.//minplayers')
                if min_players is not None:
                    details['min_players'] = min_players.get('value', '')
                
                max_players = item.find('.//maxplayers')
                if max_players is not None:
                    details['max_players'] = max_players.get('value', '')
                
                playing_time = item.find('.//playingtime')
                if playing_time is not None:
                    details['playing_time'] = playing_time.get('value', '')
                
                min_age = item.find('.//minage')
                if min_age is not None:
                    details['min_age'] = min_age.get('value', '')
                
                # Get complexity rating (averageweight)
                stats = item.find('.//statistics')
                if stats is not None:
                    avg_weight = stats.find('.//averageweight')
                    if avg_weight is not None:
                        details['complexity_rating'] = avg_weight.get('value', '')
            
            # Convert lists to strings for CSV storage
            for key in ['category', 'mechanism', 'designer', 'artist', 'publisher', 'integrates_with']:
                details[key] = '|'.join(details[key])
            
            return details
        
        elif response.status_code == 429:
            # Too many requests, wait longer and try again
            print(f"Rate limited for game {game_id}, waiting 5 seconds...")
            time.sleep(5)  # Increased from 2 to 5 seconds
            return get_game_details(game_id)
        else:
            return None
    
    except Exception as e:
        return None

def process_game(game_data):
    """Process a single game - for use with concurrent execution"""
    index, game_id, game_name = game_data
    
    # Add exponential backoff for retries
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            details = get_game_details(game_id)
            if details:
                details['name'] = game_name
                return index, details
            
            # If we get here, there was an error but no exception
            time.sleep(retry_delay)
            retry_delay *= 2
        except:
            time.sleep(retry_delay)
            retry_delay *= 2
    
    # If all retries failed, return None for this game
    return index, None

def main():
    # Start timing the execution
    start_time = time.time()
    
    # Read the existing game descriptions file to get the IDs
    try:
        descriptions_df = pd.read_csv('game_descriptions.csv')
        print(f"Loaded {len(descriptions_df)} games from descriptions file")
        
        # Process all games instead of limiting to 100
        # descriptions_df = descriptions_df.head(100)
        # print(f"Processing first 100 games for testing")
        print(f"Processing all {len(descriptions_df)} games")
        
    except Exception as e:
        print(f"Error loading game descriptions: {str(e)}")
        return

    # Check for checkpoint file
    checkpoint_file = 'details_scraping_checkpoint.json'
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
    file_mode = 'a' if start_index > 0 and os.path.exists('game_details.csv') else 'w'
    
    # Prepare the data for concurrent processing
    # Use actual DataFrame indices rather than enumeration to handle header correctly
    games_to_process = [(i, row['id'], row['name']) 
                        for i, (_, row) in enumerate(descriptions_df.iloc[start_index:].iterrows())]
    
    # Define the fieldnames for the CSV
    fieldnames = ['id', 'name', 'type', 'category', 'mechanism', 'designer', 
                 'artist', 'publisher', 'min_players', 'max_players', 
                 'playing_time', 'min_age', 'complexity_rating', 'integrates_with']
    
    # Open the output file
    with open('game_details.csv', file_mode, newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        # Write header only if starting fresh
        if file_mode == 'w':
            writer.writeheader()
        
        # Use ThreadPoolExecutor for concurrent API requests
        # Increase to 7 workers for better throughput while still avoiding rate limits
        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
            # Submit all tasks
            future_to_game = {executor.submit(process_game, game_data): game_data for game_data in games_to_process}
            
            # Process results as they complete
            completed = 0
            checkpoint_counter = 0  # Counter to track when to save checkpoint
            total = len(games_to_process)
            
            # Create a progress bar
            with tqdm(total=total, desc="Scraping game details") as pbar:
                for future in concurrent.futures.as_completed(future_to_game):
                    game_data = future_to_game[future]
                    try:
                        index, details = future.result()
                        
                        if details:
                            # Write to CSV
                            writer.writerow(details)
                            file.flush()
                            
                            # Increment checkpoint counter
                            checkpoint_counter += 1
                            
                            # Save checkpoint every 30 games
                            if checkpoint_counter >= 30:
                                with open(checkpoint_file, 'w') as f:
                                    json.dump({'last_index': start_index + index}, f)
                                checkpoint_counter = 0  # Reset counter
                                print(f"\nCheckpoint saved at game #{start_index + index}")
                        
                        completed += 1
                        pbar.update(1)
                        
                        # Print progress statistics periodically
                        if completed % 50 == 0:
                            elapsed = time.time() - start_time
                            avg_time_per_game = elapsed / completed
                            remaining_games = total - completed
                            est_time_remaining = avg_time_per_game * remaining_games
                            
                            print(f"\nProcessed {completed}/{total} games")
                            print(f"Average time per game: {avg_time_per_game:.2f} seconds")
                            print(f"Estimated time remaining: {est_time_remaining/60:.2f} minutes")
                    
                    except Exception as e:
                        print(f"Error processing game {game_data[1]}: {str(e)}")
                        pbar.update(1)
    
    # Calculate and print total execution time
    elapsed_time = time.time() - start_time
    print(f"Total time elapsed: {elapsed_time/60:.2f} minutes")
    print(f"Successfully processed {completed} out of {total} games")
    print("API scraping completed. Checkpoint saved.")

if __name__ == "__main__":
    main()