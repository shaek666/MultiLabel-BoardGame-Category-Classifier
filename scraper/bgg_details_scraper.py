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
    try:
        
        time.sleep(0.7)
        
        url = f"https://boardgamegeek.com/xmlapi2/thing?id={game_id}&stats=1"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            
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
            
            item = root.find('.//item')
            if item is not None:
                details['type'] = item.get('type', '')
                
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
                
                stats = item.find('.//statistics')
                if stats is not None:
                    avg_weight = stats.find('.//averageweight')
                    if avg_weight is not None:
                        details['complexity_rating'] = avg_weight.get('value', '')
            
            for key in ['category', 'mechanism', 'designer', 'artist', 'publisher', 'integrates_with']:
                details[key] = '|'.join(details[key])
            
            return details
        
        elif response.status_code == 429:
            print(f"Rate limited for game {game_id}, waiting 5 seconds...")
            time.sleep(5)  
            return get_game_details(game_id)
        else:
            return None
    
    except Exception as e:
        return None

def process_game(game_data):
    index, game_id, game_name = game_data
    
    max_retries = 3
    retry_delay = 1
    
    for attempt in range(max_retries):
        try:
            details = get_game_details(game_id)
            if details:
                details['name'] = game_name
                return index, details
            
            time.sleep(retry_delay)
            retry_delay *= 2
        except:
            time.sleep(retry_delay)
            retry_delay *= 2
    
    return index, None

def main():
    start_time = time.time()
    
    try:
        descriptions_df = pd.read_csv('game_descriptions.csv')
        print(f"Loaded {len(descriptions_df)} games from descriptions file")
        
        print(f"Processing all {len(descriptions_df)} games")
        
    except Exception as e:
        print(f"Error loading game descriptions: {str(e)}")
        return

    checkpoint_file = 'details_scraping_checkpoint.json'
    start_index = 0
    
    if os.path.exists(checkpoint_file):
        try:
            with open(checkpoint_file, 'r') as f:
                checkpoint_data = json.load(f)
                start_index = checkpoint_data.get('last_index', 0) + 1
                print(f"Resuming from game #{start_index}")
        except json.JSONDecodeError:
            print("Checkpoint file corrupted. Starting from the beginning.")
            start_index = 0
    
    file_mode = 'a' if start_index > 0 and os.path.exists('game_details.csv') else 'w'
    
    games_to_process = [(i, row['id'], row['name']) 
                        for i, (_, row) in enumerate(descriptions_df.iloc[start_index:].iterrows())]
    
    fieldnames = ['id', 'name', 'type', 'category', 'mechanism', 'designer', 
                 'artist', 'publisher', 'min_players', 'max_players', 
                 'playing_time', 'min_age', 'complexity_rating', 'integrates_with']
    
    with open('game_details.csv', file_mode, newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        
        if file_mode == 'w':
            writer.writeheader()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=7) as executor:
            future_to_game = {executor.submit(process_game, game_data): game_data for game_data in games_to_process}
            
            completed = 0
            checkpoint_counter = 0 
            total = len(games_to_process)
            
            with tqdm(total=total, desc="Scraping game details") as pbar:
                for future in concurrent.futures.as_completed(future_to_game):
                    game_data = future_to_game[future]
                    try:
                        index, details = future.result()
                        
                        if details:
                            writer.writerow(details)
                            file.flush()
                            
                            checkpoint_counter += 1
                            
                            if checkpoint_counter >= 30:
                                with open(checkpoint_file, 'w') as f:
                                    json.dump({'last_index': start_index + index}, f)
                                checkpoint_counter = 0  # Reset counter
                                print(f"\nCheckpoint saved at game #{start_index + index}")
                        
                        completed += 1
                        pbar.update(1)
                        
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
    
    elapsed_time = time.time() - start_time
    print(f"Total time elapsed: {elapsed_time/60:.2f} minutes")
    print(f"Successfully processed {completed} out of {total} games")
    print("API scraping completed. Checkpoint saved.")

if __name__ == "__main__":
    main()