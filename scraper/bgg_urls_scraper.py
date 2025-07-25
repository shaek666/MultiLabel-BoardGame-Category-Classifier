from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import concurrent.futures
import requests

def setup_driver():
    """Setup Chrome WebDriver with minimal options for speed"""
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    service = Service(ChromeDriverManager().install())
    return webdriver.Chrome(service=service, options=options)

def verify_url_fast(url):
    """Quick check if URL exists using requests"""
    try:
        response = requests.head(url, timeout=1)  # Fast timeout
        # Accept 200 (OK), 301 (Moved Permanently), and 302 (Found) as valid
        return response.status_code in [200, 301, 302]
    except Exception as e:
        # If there's an error, retry once
        try:
            response = requests.head(url, timeout=2)  # Slightly longer timeout on retry
            return response.status_code in [200, 301, 302]
        except:
            return False

def process_batch(urls_batch):
    """Process a batch of URLs in parallel"""
    valid_urls = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:  # Using 100 workers
        futures = {executor.submit(verify_url_fast, url['url']): url for url in urls_batch}
        for future in concurrent.futures.as_completed(futures):
            url_data = futures[future]
            if future.result():
                valid_urls.append(url_data)
    return valid_urls

def scrape_game_urls_fast():
    # Read data and prepare URLs
    df = pd.read_csv('games_merged_sorted.csv')
    
    # Take the first 30,000 games and maintain order
    df = df.head(30000)
    
    # Create URLs directly without any verification
    urls_data = [
        {'id': row['id'], 'name': row['name'], 
         'url': f"https://boardgamegeek.com/boardgame/{row['id']}"}
        for _, row in df[['id', 'name']].iterrows()
    ]
    
    # Save all URLs immediately without any checks
    pd.DataFrame(urls_data).to_csv('game_urls.csv', index=False)
    print(f"Saved {len(urls_data)} game URLs to game_urls.csv")

if __name__ == '__main__':
    scrape_game_urls_fast()