import json
import requests
import pandas as pd
import time
import yfinance as yf
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import random
from sqlalchemy import create_engine, text, inspect, Engine
import traceback
import logging
import queue
import os # <-- Import os for environment variables
from dotenv import load_dotenv # <-- Import dotenv
from dataclasses import dataclass
from typing import Optional, Tuple, Set
from fredapi import Fred

# --- Load Environment Variables ---
load_dotenv()

# --- Data Structures & Setup ---
@dataclass
class FailedTicker:
    ticker: str
    reason: str
    error_type: str
    attempts: int = 1

def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO) 
    if logger.hasHandlers(): logger.handlers.clear()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    fh = logging.FileHandler('data_pipeline.log', mode='w', encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def load_config(logger, config_path='Data Collection/config.json'):
    logger.info(f"Loading configuration from {config_path}...")
    try:
        with open(config_path, 'r', encoding='utf-8') as f: config = json.load(f)
        logger.info("✅ Configuration loaded successfully.")
        return config
    except Exception as e:
        logger.critical(f"❌ FATAL ERROR loading config: {e}")
        return None

def create_tables_if_not_exist(engine, logger):
    logger.info("--- Checking/Creating Database Tables ---")
    try:
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text("""
                    CREATE TABLE IF NOT EXISTS stocks (
                        ticker VARCHAR(20) PRIMARY KEY, sector VARCHAR(100), industry VARCHAR(100),
                        market_cap_category VARCHAR(50), market_cap BIGINT
                    );"""))
                connection.execute(text("""
                    CREATE TABLE IF NOT EXISTS daily_stock_data (
                        id SERIAL PRIMARY KEY, ticker VARCHAR(20) REFERENCES stocks(ticker) ON DELETE CASCADE,
                        date DATE NOT NULL, open NUMERIC(10, 2), high NUMERIC(10, 2), low NUMERIC(10, 2),
                        close NUMERIC(10, 2), adj_close NUMERIC(10, 2), volume BIGINT, UNIQUE(ticker, date)
                    );"""))
                connection.execute(text("CREATE INDEX IF NOT EXISTS idx_daily_stock_data_ticker_date ON daily_stock_data(ticker, date);"))
                connection.execute(text("""
                    CREATE TABLE IF NOT EXISTS macro_data (
                        date DATE NOT NULL, series_id VARCHAR(20) NOT NULL, value NUMERIC, PRIMARY KEY (date, series_id)
                    );"""))
    except Exception as e:
        logger.error(f"❌ ERROR creating tables: {e}"); raise

# --- Helper Functions ---
def categorize_market_cap(market_cap):
    if market_cap is None: return 'Unknown'
    if market_cap >= 10_000_000_000: return 'Large-Cap'
    elif market_cap >= 2_000_000_000: return 'Mid-Cap'
    else: return 'Small-Cap'

def fetch_stock_universe(config, logger):
    logger.info("--- Phase 1 & 2: Fetching and Filtering Stock Universe ---")
    tv_config = config['trading_view']
    all_stocks = []
    for exchange in tv_config['exchanges']:
        logger.info(f"Fetching stocks for {exchange}...")
        try:
            payload = {"filter": [{"left": "exchange", "operation": "equal", "right": exchange}], "columns": ["name", "sector", "industry", "market_cap_basic", "average_volume_10d_calc"], "range": [0, tv_config['max_stocks_per_exchange']]}
            response = requests.post("https://scanner.tradingview.com/america/scan", json=payload, timeout=30)
            response.raise_for_status()
            data = response.json().get("data", [])
            qualified_stocks = [{'ticker': item["s"].split(":", 1)[-1], 'sector': d[1] or 'Unknown', 'industry': d[2] or 'Unknown', 'market_cap_category': categorize_market_cap(d[3]), 'market_cap': int(d[3]) if d[3] else 0} for item in data if (d := item.get('d', [])) and len(d) >= 5 and (d[3] is not None and d[3] > tv_config['min_market_cap']) and (d[4] is not None and d[4] > tv_config['min_avg_volume'])]
            logger.info(f"Found {len(data)} stocks, {len(qualified_stocks)} met quality criteria.")
            all_stocks.extend(qualified_stocks)
        except Exception as e:
            logger.error(f"❌ ERROR fetching {exchange}: {e}")
    if not all_stocks: logger.critical("❌ No stocks found!"); return None
    df = pd.DataFrame(all_stocks).drop_duplicates(subset=['ticker']).dropna()
    df = df[~df['ticker'].str.contains(r'[./]', regex=True)]
    logger.info(f"✅ Phase 1 & 2 Complete: Found {len(df)} unique, high-quality common stocks.")
    return df

def fetch_and_store_macro_data(engine, config, logger):
    logger.info("--- Fetching Macroeconomic Data from FRED ---")
    # --- SECURE: Read key from environment variable ---
    api_key = os.getenv("FRED_API_KEY")
    if not api_key:
        logger.warning("⚠️ FRED_API_KEY not found in environment variables. Skipping macro data.")
        return
    try:
        fred = Fred(api_key=api_key)
        series_to_fetch = config.get('fred_api', {}).get('series', {}).keys()
        if not series_to_fetch: logger.warning("⚠️ No FRED series defined in config. Skipping."); return
        
        all_macro_data = []
        for series_id in series_to_fetch:
            data = fred.get_series(series_id)
            df = data.reset_index(); df.columns = ['date', 'value']; df['series_id'] = series_id
            all_macro_data.append(df)
        
        if not all_macro_data: logger.warning("No macro data returned from FRED."); return
        final_df = pd.concat(all_macro_data, ignore_index=True).dropna()
        final_df['date'] = pd.to_datetime(final_df['date']).dt.date
        
        logger.info(f"Upserting {len(final_df)} macro data points to the database.")
        with engine.connect() as connection:
            with connection.begin():
                for row in final_df.itertuples():
                    stmt = text("""INSERT INTO macro_data (date, series_id, value) VALUES (:date, :series_id, :value)
                                   ON CONFLICT (date, series_id) DO UPDATE SET value = EXCLUDED.value;""")
                    connection.execute(stmt, {'date': row.date, 'series_id': row.series_id, 'value': row.value})
        logger.info("✅ Macro data updated successfully.")
    except Exception as e:
        logger.error(f"❌ ERROR fetching/storing macro data: {e}")

# --- Incremental Update Helper Functions ---
def get_db_state(engine, logger) -> Tuple[Set[str], Optional[pd.Timestamp]]:
    logger.info("--- Checking current database state ---")
    with engine.connect() as connection:
        latest_date_result = connection.execute(text("SELECT MAX(date) FROM daily_stock_data;")).scalar()
        latest_date = pd.to_datetime(latest_date_result) if latest_date_result else None
        existing_tickers_result = connection.execute(text("SELECT DISTINCT ticker FROM daily_stock_data;")).fetchall()
        existing_tickers = {row[0] for row in existing_tickers_result}
    logger.info(f"Latest data in DB is for: {latest_date.strftime('%Y-%m-%d') if latest_date else 'No data found'}")
    logger.info(f"Found {len(existing_tickers)} tickers with existing data.")
    return existing_tickers, latest_date

def identify_stocks_for_full_refresh(tickers_to_check, logger) -> Set[str]:
    logger.info(f"--- Checking {len(tickers_to_check)} existing stocks for recent corporate actions ---")
    stocks_to_refresh = set()
    check_since_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')
    def check_actions(ticker):
        try:
            stock = yf.Ticker(ticker)
            actions = stock.actions
            if not actions.empty and actions.index.max() >= pd.Timestamp(check_since_date):
                if ('Stock Splits' in actions and actions['Stock Splits'].sum() != 0) or \
                   ('Dividends' in actions and actions['Dividends'].sum() > 0):
                    return ticker
        except Exception: return ticker
        return None
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(check_actions, ticker) for ticker in tickers_to_check}
        for future in tqdm(as_completed(futures), total=len(futures), desc="Checking for splits/dividends"):
            result = future.result()
            if result: stocks_to_refresh.add(result)
    if stocks_to_refresh: logger.warning(f"⚠️ Identified {len(stocks_to_refresh)} stocks with recent actions requiring a full refresh.")
    else: logger.info("✅ No recent corporate actions found.")
    return stocks_to_refresh

# --- Data Fetching Worker and Orchestrator ---
def fetch_and_store_single_stock(ticker: str, start_str: str, end_str: str, db_engine: Engine, failed_tickers_queue: queue.Queue):
    try:
        stock = yf.Ticker(ticker)
        hist_df = stock.history(start=start_str, end=end_str, auto_adjust=False)
        if hist_df.empty: return True
        
        hist_df.reset_index(inplace=True)
        hist_df.columns = [str(col).lower().replace(' ', '_') for col in hist_df.columns]
        hist_df.rename(columns={'adj_close': 'adj_close'}, inplace=True, errors='ignore')
        if 'adj_close' not in hist_df.columns: hist_df['adj_close'] = hist_df['close']
        
        required_cols = ['date', 'open', 'high', 'low', 'close', 'adj_close', 'volume', 'ticker']
        hist_df['ticker'] = ticker
        hist_df['date'] = pd.to_datetime(hist_df['date']).dt.date
        clean_df = hist_df[required_cols].dropna()
        
        if clean_df.empty: return True

        clean_df[['open', 'high', 'low', 'close', 'adj_close']] = clean_df[['open', 'high', 'low', 'close', 'adj_close']].round(2)
        
        with db_engine.connect() as connection:
            with connection.begin():
                for row in clean_df.itertuples(index=False):
                    params = {
                        'date': row.date,
                        'open': row.open,
                        'high': row.high,
                        'low': row.low,
                        'close': row.close,
                        'adj_close': row.adj_close,
                        'volume': row.volume,
                        'ticker': row.ticker
                    }
                    stmt = text("""
                        INSERT INTO daily_stock_data (date, open, high, low, close, adj_close, volume, ticker)
                        VALUES (:date, :open, :high, :low, :close, :adj_close, :volume, :ticker)
                        ON CONFLICT (ticker, date) DO UPDATE SET
                            open = EXCLUDED.open, high = EXCLUDED.high, low = EXCLUDED.low,
                            close = EXCLUDED.close, adj_close = EXCLUDED.adj_close, volume = EXCLUDED.volume;
                    """)
                    connection.execute(stmt, params)
        return True
    except Exception as e:
        failed_tickers_queue.put(FailedTicker(ticker, f"General error: {str(e)[:150]}", "permanent"))
        return False

def fetch_historical_data(stock_universe_df, config, engine, logger):
    logger.info("--- Phase 3: Fetching Historical Market Data (Incremental Update) ---")
    all_tickers_in_universe = set(stock_universe_df['ticker'].tolist())
    
    existing_tickers, latest_date_in_db = get_db_state(engine, logger)
    stocks_needing_full_refresh = identify_stocks_for_full_refresh(existing_tickers, logger)

    new_tickers = all_tickers_in_universe - existing_tickers
    stocks_to_fully_refresh = new_tickers.union(stocks_needing_full_refresh)
    stocks_to_update_incrementally = all_tickers_in_universe - stocks_to_fully_refresh
    
    logger.info(f"Update plan: {len(stocks_to_fully_refresh)} for full refresh, {len(stocks_to_update_incrementally)} for incremental update.")

    if stocks_needing_full_refresh:
        with engine.connect() as connection:
            with connection.begin():
                # First, log the tickers that need a full analytics recalculation.
                logger.info(f"Logging {len(stocks_needing_full_refresh)} tickers for analytics refresh...")
                for ticker in stocks_needing_full_refresh:
                    stmt = text("INSERT INTO refreshed_tickers_log (ticker) VALUES (:ticker) ON CONFLICT (ticker) DO NOTHING;")
                    connection.execute(stmt, {'ticker': ticker})
                
                # Now, delete their raw data to prepare for re-download.
                logger.warning(f"Deleting existing raw data for {len(stocks_needing_full_refresh)} stocks before refresh...")
                ticker_list = list(stocks_needing_full_refresh)
                for i in range(0, len(ticker_list), 100):
                    chunk = ticker_list[i:i+100]
                    stmt = text("DELETE FROM daily_stock_data WHERE ticker = ANY(:tickers_to_delete)")
                    connection.execute(stmt, {'tickers_to_delete': chunk})
            logger.info("Logging and deletion complete.")

    failed_tickers_queue = queue.Queue()
    full_refresh_start_date = (datetime.now() - timedelta(days=365.25 * config['yfinance']['years_of_data'])).strftime('%Y-%m-%d')
    incremental_start_date = (latest_date_in_db + timedelta(days=1)).strftime('%Y-%m-%d') if latest_date_in_db else full_refresh_start_date
    end_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')

    tasks = []
    for ticker in stocks_to_fully_refresh: tasks.append({'ticker': ticker, 'start': full_refresh_start_date, 'end': end_date})
    for ticker in stocks_to_update_incrementally: tasks.append({'ticker': ticker, 'start': incremental_start_date, 'end': end_date})

    success_count = 0
    with ThreadPoolExecutor(max_workers=config['pipeline_settings']['max_workers']) as executor:
        future_to_task = { executor.submit(fetch_and_store_single_stock, task['ticker'], task['start'], task['end'], engine, failed_tickers_queue): task for task in tasks }
        for future in tqdm(as_completed(future_to_task), total=len(tasks), desc="Fetching & Storing Data"):
            if future.result(): success_count += 1
    
    logger.info(f"✅ Data fetch complete: {success_count} tasks succeeded.")
    return True

# --- Main Orchestrator ---
def main():
    logger = setup_logging()
    config = load_config(logger)
    if not config: return

    try:
        db_config = config['database']
        engine = create_engine(f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}", pool_pre_ping=True)
        create_tables_if_not_exist(engine, logger)
        fetch_and_store_macro_data(engine, config, logger)
    except Exception as e:
        logger.critical(f"❌ FATAL ERROR during database setup: {e}"); return

    stock_universe_df = fetch_stock_universe(config, logger)
    if stock_universe_df is None or stock_universe_df.empty:
        logger.critical("❌ Stock universe is empty. Halting pipeline."); return

    benchmark_ticker = config['yfinance']['benchmark_ticker']
    if benchmark_ticker not in stock_universe_df['ticker'].values:
        logger.info(f"Adding benchmark ticker '{benchmark_ticker}' to the universe for download.")
        benchmark_row = pd.DataFrame([{
            'ticker': benchmark_ticker,
            'sector': 'Index',
            'industry': 'Market Index',
            'market_cap_category': 'N/A',
            'market_cap': 0
        }])
        stock_universe_df = pd.concat([stock_universe_df, benchmark_row], ignore_index=True)
    try:
        logger.info("Updating 'stocks' master table...")
        with engine.connect() as connection:
            with connection.begin():
                connection.execute(text("TRUNCATE TABLE stocks RESTART IDENTITY CASCADE;"))
        stock_universe_df.to_sql('stocks', con=engine, if_exists='append', index=False)
        logger.info("✅ 'stocks' table populated with the latest universe.")
    except Exception as e:
        logger.error(f"❌ ERROR during stocks table population: {e}"); return

    success = fetch_historical_data(stock_universe_df, config, engine, logger)
    
    if success:
        logger.info("--- Pipeline Completed Successfully ---")
    else:
        logger.error("❌ Pipeline failed to complete successfully.")

if __name__ == "__main__":
    main()