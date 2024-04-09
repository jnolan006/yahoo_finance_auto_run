import requests
import pandas as pd
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup
import pytz
import matplotlib.pyplot as plt
import os
import datetime
import psycopg2
import yfinance as yf

# Get the current date
current_date = datetime.date.today() - datetime.timedelta(days=2)
end_date = datetime.date.today()
# Format the date as a string in the format 'YYYY-MM-DD'
starting_date = current_date.strftime('%Y-%m-%d')
print(starting_date)
# Connect to database
host = "ec2-52-6-117-96.compute-1.amazonaws.com"
dbname = "dftej5l5m1cl78"
user = "aiuhlrpcnftsjs"
password = "8b2220cd5b6da572369545d91f6b435dfc37a42bfec6b6e2a5c9f236dfb65f42"

conn = psycopg2.connect(host=host, dbname=dbname, user=user, password=password)
cur = conn.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS yahoo_sentiment_v_price (date DATE, symbol VARCHAR, close REAL, open REAL, bullish_count REAL, bearish_count REAL)")

all_symbols = ['JPM', 'BAC', 'WFC', 'HDB', 'HSBC', 'MS', 'RY', 'GS', 'SCHW', 'TD', 'AAPL', 'GOOG', 'AMZN', 'MSFT', 'TSLA', 'CRM', 'AMD', 'BABA', 'INTC', 'ATVI', 'PYPL', 'META', 'TTD', 'EA', 'ZG', 'MTCH', 'YELP', 'TIVO', 'MUFG', 'C', 'UBS', 'IBN', 'SMFG', 'BNPQY', 'BMO', 'ITUB', 'USB', 'BBVA', 'BNS', 'PNC', 'UNCRY', 'NU', 'ING', 'TFC', 'IBKR', 'BSBR', 'BCS', 'DB']

price_data_df = pd.DataFrame()
for stock_symbol in all_symbols:
    price_data_df_prep = yf.download(stock_symbol, start=starting_date, end=end_date)
    price_data_df_prep['symbol'] = stock_symbol
    price_data_df_prep['starting_date'] = starting_date
    price_data_df = pd.concat([price_data_df, price_data_df_prep[['symbol', 'starting_date', 'Open', 'Close']]])

def scrape_yahoo_trending_tickers(url):
    # Send a GET request to the URL
    response = requests.get(url)
    # Check if the request was successful
    if response.status_code == 200:
        # Parse the HTML content using BeautifulSoup
        soup = BeautifulSoup(response.text, 'html.parser')
        # print(soup.prettify())
        symbol_list = []
        # Extract data using the provided XPath
        for link in soup.find_all('a'):
            symbol = (link.get('href'))
            if '/chart/' in symbol and '%' not in symbol:
                split_html = symbol.split('/')
                symbol = split_html[-1]
                symbol_list.append(symbol)

        return symbol_list

result_df = pd.DataFrame(columns=['symbol', 'bullish_count', 'bearish_count', 'diff'])
all_pulls = []
symbol_list = []
for _ in range(10):
    # Convert UTC timestamp to PST
    current_time_utc = pd.Timestamp.utcnow()
    pst_timezone = pytz.timezone('America/Los_Angeles')
    current_time_pst = current_time_utc.tz_convert(pst_timezone)
    # URL of the Yahoo Finance trending tickers page
    yahoo_trending_tickers_url = 'https://finance.yahoo.com/trending-tickers'
    # Call the function with the URL
    current_symbols = []
    test = scrape_yahoo_trending_tickers(yahoo_trending_tickers_url)
    # test = [symbol for symbol in test if symbol not in current_symbols]
    test = ['JPM', 'BAC', 'WFC', 'HDB', 'HSBC', 'MS', 'RY', 'GS', 'SCHW', 'TD', 'AAPL', 'GOOG', 'AMZN', 'MSFT', 'TSLA', 'CRM', 'AMD', 'BABA', 'INTC', 'ATVI', 'PYPL', 'META', 'TTD', 'EA', 'ZG', 'MTCH', 'YELP', 'TIVO', 'MUFG', 'C', 'UBS', 'IBN', 'SMFG', 'BNPQY', 'BMO', 'ITUB', 'USB', 'BBVA', 'BNS', 'PNC', 'UNCRY', 'NU', 'ING', 'TFC', 'IBKR', 'BSBR', 'BCS', 'DB']
    for symbol in current_symbols:
        test.append(symbol)
    previous_pull = []
    for symbol in test:
        if symbol not in symbol_list:
            symbol_list.append(symbol)
    for ticker in symbol_list:
        # Get the data from the Yahoo Finance community
        url = f'https://finance.yahoo.com/quote/{ticker}/community?p=TSL{ticker}'
        response = requests.get(url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/110.0'})
        soup = BeautifulSoup(response.text, 'html.parser')
        spotim_config = soup.select_one('#spotim-config')

        if spotim_config is not None:
            data = json.loads(spotim_config.get_text(strip=True))['config']

            url = "https://api-2-0.spot.im/v1.0.0/conversation/read"
            payload = json.dumps({
              "conversation_id": data['spotId'] + data['uuid'].replace('_', '$'),
              "count": 250,
              "offset": 0
            })
            headers = {
              'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/110.0',
              'Content-Type': 'application/json',
              'x-spot-id': data['spotId'],
              'x-post-id': data['uuid'].replace('_', '$'),
            }

            # Send the API request
            response = requests.post(url, headers=headers, data=payload)
            data = response.json()

            # Initialize counters for Bullish and Bearish labels
            bullish_count = 0
            bearish_count = 0
            current_time = datetime.datetime.now()

            for comment in data.get('conversation', {}).get('comments', []):
                written_at = comment.get('written_at')
                written_at_time = datetime.datetime.fromtimestamp(written_at)
                # Calculate time difference in hours
                time_diff_hours = (current_time - written_at_time).total_seconds() / 3600
                if time_diff_hours <= 12: 
                    if 'content' in comment:
                        content_text = ' '.join([item['text'] for item in comment['content'] if item.get('type') == 'text'])
                        previous_pull.append(content_text)
                        if 'additional_data' in comment and 'labels' in comment['additional_data'] and 'ids' in comment['additional_data']['labels'] and content_text not in all_pulls:
                            labels = comment['additional_data']['labels']['ids']
                            if 'BULLISH' in labels:
                                bullish_count += 1
                            if 'BEARISH' in labels:
                                bearish_count += 1


            df = pd.DataFrame({
                'symbol': [ticker],
                'bullish_count': [bullish_count],
                'bearish_count': [bearish_count],
                'diff': [abs(bullish_count - bearish_count)],
            })

            # Append df to result_df
            result_df = pd.concat([result_df, df], ignore_index=True)
    result_df = result_df.sort_values(by=['symbol'], ascending=True)
    all_pulls = previous_pull
    # Group the DataFrame by 'Symbol'
    grouped_df = result_df.groupby('symbol')

    # Calculate the sum of 'Diff' values for each group
    sum_of_diff = grouped_df[['bullish_count', 'bearish_count', 'diff']].sum()

    time.sleep(30 * 60)

merged_df = pd.merge(price_data_df, sum_of_diff, on='symbol', how='inner')
for index, row in merged_df.iterrows():
    # Extract data from the row
    date = row['starting_date']
    symbol = row['symbol']
    close = row['Close']
    open = row['Open']
    bullish_count = row['bullish_count']
    bearish_count = row['bearish_count']
    
    # Execute the SQL INSERT statement
    cur.execute("INSERT INTO yahoo_sentiment_v_price (date, symbol, close, open, bullish_count, bearish_count) VALUES (%s, %s, %s, %s, %s, %s)",
                (date, symbol, close, open, bullish_count, bearish_count))

# Commit the changes to the database
conn.commit()

# Close the cursor and database connection
cur.close()
conn.close()
