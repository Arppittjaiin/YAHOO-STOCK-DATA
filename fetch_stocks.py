import pandas as pd
import yfinance as yf
import os
import re
from datetime import timedelta, datetime
from tqdm import tqdm
import time

# Configuration
SOURCE_FILE = 'EQUITY_L.csv'

# Define intervals and their maximum fetchable history (yfinance limits)
# 'max' indicates full history available. 
# Days are approx. yfinance API limits as of late 2024.
INTERVAL_LIMITS = {
    '1m': '7d',
    '2m': '60d',
    '5m': '60d',
    '15m': '60d',
    '30m': '60d',
    '60m': '730d', # ~2 years
    '90m': '60d',
    '1h': '730d',
    '1d': 'max',
    '5d': 'max',
    '1wk': 'max',
    '1mo': 'max',
    '3mo': 'max'
}

def sanitize_filename(name):
    # Remove invalid characters for Windows filenames
    return re.sub(r'[<>:"/\\|?*]', '', str(name)).strip()

def fetch_and_update(ticker_symbol, company_name):
    # Create directory for the company
    clean_name = sanitize_filename(company_name)
    company_dir = os.path.join(os.getcwd(), clean_name)
    
    if not os.path.exists(company_dir):
        os.makedirs(company_dir)
        
    symbol_ns = f"{ticker_symbol}.NS"
    
    for interval, max_period in INTERVAL_LIMITS.items():
        csv_file = os.path.join(company_dir, f"{ticker_symbol}_{interval}.csv")
        
        try:
            if os.path.exists(csv_file):
                # Load existing data to find last date
                try:
                    existing_df = pd.read_csv(csv_file, index_col=0, parse_dates=True)
                    # Clean existing data
                    if existing_df.index.has_duplicates:
                        existing_df = existing_df[~existing_df.index.duplicated(keep='last')]
                    if existing_df.columns.has_duplicates:
                        existing_df = existing_df.loc[:, ~existing_df.columns.duplicated()]

                    if existing_df.empty:
                        last_date = None
                    else:
                        last_date = existing_df.index[-1]
                except Exception as e:
                    print(f"Error reading {csv_file}: {e}. Re-downloading full allow.")
                    last_date = None
                    existing_df = pd.DataFrame()
                
                if last_date:
                    # Determine start date (last_date + small buffer to ensure continuity)
                    # For intraday, we just want "from last known". 
                    # If last known is too old (e.g. 8 days ago for 1m), we can't bridge the gap due to API limits.
                    # In that case, we just fetch what we can and append (leaving a gap).
                    
                    # Logic: Fetch from last_date. Use 'start' parameter if supported for the interval and range.
                    # However, yfinance 'start' param for intraday often throws errors if > limit.
                    # Safer approach for intraday: Fetch max allowed period ('7d' etc) and filter/merge.
                    
                    if interval in ['1d', '5d', '1wk', '1mo', '3mo']:
                        # Standard handling for daily+
                        start_date = last_date + timedelta(days=1)
                        if start_date < datetime.now(start_date.tzinfo):
                            # print(f"Updating {symbol_ns} [{interval}] from {start_date}...")
                            new_data = yf.download(symbol_ns, start=start_date, interval=interval, progress=False, auto_adjust=True)
                            if isinstance(new_data.columns, pd.MultiIndex):
                                new_data.columns = new_data.columns.droplevel(1)
                            
                            if not new_data.empty:
                                # Clean new data
                                if new_data.index.has_duplicates:
                                    new_data = new_data[~new_data.index.duplicated(keep='last')]
                                if new_data.columns.has_duplicates:
                                    new_data = new_data.loc[:, ~new_data.columns.duplicated()]

                                # Align timezones
                                try:
                                    if not existing_df.empty:
                                        if existing_df.index.tz is None and new_data.index.tz is not None:
                                            existing_df.index = existing_df.index.tz_localize(new_data.index.tz)
                                        elif existing_df.index.tz is not None and new_data.index.tz is None:
                                            new_data.index = new_data.index.tz_localize(existing_df.index.tz)
                                    
                                    combined = pd.concat([existing_df, new_data])
                                    combined = combined[~combined.index.duplicated(keep='last')]
                                    combined.to_csv(csv_file)
                                except Exception as e_concat:
                                     print(f"Concat failed for {symbol_ns} {interval}: {e_concat}")
                    else:
                        # Intraday handling
                        # We cannot legally ask for 'start=2020-01-01' for 1m data.
                        # We fetch the max_period allowed (e.g. '7d') and merge.
                        # This ensures we get the latest data. Gaps may occur if script isn't run often enough.
                        
                        # print(f"Updating {symbol_ns} [{interval}] (Rolling Fetch)...")
                        new_data = yf.download(symbol_ns, period=max_period, interval=interval, progress=False, auto_adjust=True)
                        if isinstance(new_data.columns, pd.MultiIndex):
                            new_data.columns = new_data.columns.droplevel(1)
                            
                        if not new_data.empty:
                            # Clean new data
                            if new_data.index.has_duplicates:
                                new_data = new_data[~new_data.index.duplicated(keep='last')]
                            if new_data.columns.has_duplicates:
                                new_data = new_data.loc[:, ~new_data.columns.duplicated()]

                            try:
                                # Align timezones
                                if not existing_df.empty:
                                    if existing_df.index.tz is None and new_data.index.tz is not None:
                                       existing_df.index = existing_df.index.tz_localize(new_data.index.tz)
                                    elif existing_df.index.tz is not None and new_data.index.tz is None:
                                       new_data.index = new_data.index.tz_localize(existing_df.index.tz)

                                combined = pd.concat([existing_df, new_data])
                                combined = combined[~combined.index.duplicated(keep='last')]
                                combined.to_csv(csv_file)
                            except Exception as e_concat:
                                print(f"DEBUG: Concat failed for {symbol_ns} {interval}: {e_concat}")
                                # print(f"Existing columns: {existing_df.columns}")
                                # print(f"New columns: {new_data.columns}")
                                # raise e_concat

                else:
                    # File exists but empty/invalid
                    # Fetch max allowed
                    data = yf.download(symbol_ns, period=max_period, interval=interval, progress=False, auto_adjust=True)
                    if isinstance(data.columns, pd.MultiIndex):
                        data.columns = data.columns.droplevel(1)
                    if not data.empty:
                         # Clean data
                        if data.index.has_duplicates:
                            data = data[~data.index.duplicated(keep='last')]
                        if data.columns.has_duplicates:
                            data = data.loc[:, ~data.columns.duplicated()]
                        data.to_csv(csv_file)

            else:
                # File doesn't exist, fetch max allowed
                # print(f"Fetching full {symbol_ns} [{interval}]...")
                data = yf.download(symbol_ns, period=max_period, interval=interval, progress=False, auto_adjust=True)
                if isinstance(data.columns, pd.MultiIndex):
                        data.columns = data.columns.droplevel(1)
                if not data.empty:
                    data.to_csv(csv_file)
                    
        except Exception as e:
            print(f"Failed to fetch {symbol_ns} {interval}: {e}")
            pass

import concurrent.futures
import random

# Threading configuration
MAX_WORKERS = 4  # Safe number to avoid aggressive rate limiting (429)

def fetch_worker(args):
    """Worker function for threading"""
    symbol, company_name = args
    # Random small sleep to desynchronize threads slightly
    time.sleep(random.uniform(0.1, 0.5))
    fetch_and_update(symbol, company_name)
    return f"{symbol} done"

def main():
    if not os.path.exists(SOURCE_FILE):
        print(f"Source file {SOURCE_FILE} not found!")
        return

    df = pd.read_csv(SOURCE_FILE)
    total_stocks = len(df)
    
    # Shuffle to avoid hitting same bad tickers consecutively or server hotspots
    # df = df.sample(frac=1).reset_index(drop=True)
    
    print(f"Found {total_stocks} stocks. Starting update process with {MAX_WORKERS} threads...")
    print(f"Timeframes: {list(INTERVAL_LIMITS.keys())}")
    
    # Prepare arguments for mapper
    tasks = [(row['SYMBOL'], row['NAME OF COMPANY']) for index, row in df.iterrows()]
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Use simple map or futures
        # We use futures to update progress bar
        futures = {executor.submit(fetch_worker, task): task for task in tasks}
        
        for future in tqdm(concurrent.futures.as_completed(futures), total=total_stocks, unit="stock"):
            try:
                future.result()
            except Exception as e:
                # Log error but don't stop
                print(f"Task failed: {e}")
                pass


if __name__ == "__main__":
    main()
