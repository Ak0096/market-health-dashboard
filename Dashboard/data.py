import pandas as pd
from sqlalchemy import create_engine
import json
import logging
import os

# --- SETUP ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def load_config(config_path='Data Collection/config.json'):
    """Loads the database configuration from the project root."""
    logging.info("Loading configuration...")
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logging.info("✅ Configuration loaded.")
        return config
    except FileNotFoundError:
        logging.critical(f"❌ FATAL ERROR: config.json not found. Make sure you are running the dashboard from your project's root folder.")
        return None
    except Exception as e:
        logging.critical(f"❌ FATAL ERROR loading config: {e}")
        return None

def connect_to_db(config):
    try:
        db_config = config['database']
        engine_url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        engine = create_engine(engine_url)
        logging.info("✅ Database connection successful.")
        return engine
    except Exception as e:
        logging.critical(f"❌ Database connection failed: {e}")
        return None

# --- Main Data Loading Section ---
logging.info("--- Initializing Data Module ---")
config = load_config()
engine = connect_to_db(config)

# --- REFACTORED: Removed breadth_df and trend_pct_df from global scope ---
market_indicators_df, spx_df, heatmap_df, group_analytics_df, latest_stock_analytics_df, breakout_stocks_df, stock_metadata_df = (pd.DataFrame() for _ in range(7))
industry_to_sector_map = {}
latest_date = pd.Timestamp.now()
total_stocks_latest = 0
latest_df = pd.DataFrame()

if engine:
    try:
        # --- PART A: Load LATEST data for the gauges (This is a small, efficient query) ---
        latest_data_query = """
        WITH latest_date AS (SELECT MAX(date) as max_date FROM daily_stock_analytics)
        SELECT d.ticker, d.adj_close, a.ma_20, a.ma_50, a.ma_200
        FROM daily_stock_data d JOIN daily_stock_analytics a ON d.ticker = a.ticker AND d.date = a.date
        WHERE d.date = (SELECT max_date FROM latest_date);
        """
        latest_df = pd.read_sql_query(latest_data_query, con=engine)
        total_stocks_latest = len(latest_df)
        latest_date_df = pd.read_sql_query("SELECT MAX(date) as max_date FROM daily_stock_analytics", con=engine)
        if not latest_date_df.empty and pd.notna(latest_date_df['max_date'][0]):
            latest_date = latest_date_df['max_date'][0]

        # --- REMOVED: Parts B and C ---
        # The logic to load the entire historical dataset into memory has been removed.
        # This prevents the application from crashing due to high memory usage.
        # These calculations are now performed efficiently inside the relevant callback
        # using direct, targeted SQL queries.
        
        # --- PART D: Load Consolidated Market Indicators and S&P 500 Data ---
        market_indicators_df = pd.read_sql_query("SELECT * FROM daily_market_indicators ORDER BY date", con=engine, index_col='date', parse_dates=['date'])
        spx_query = "SELECT date, hlcc4 FROM daily_stock_analytics WHERE ticker = '^GSPC' ORDER BY date"
        spx_df = pd.read_sql_query(spx_query, con=engine, index_col='date', parse_dates=['date'])
        logging.info(f"✅ Loaded {len(market_indicators_df)} rows of consolidated market indicators.")

        # --- PART E: Load Data for Sector Heatmap ---
        heatmap_query = """
        WITH latest_analytics AS (
            SELECT
                ticker,
                ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY date DESC) as rn,
                perf_1w, perf_1m, perf_3m, perf_6m, perf_ytd
            FROM daily_stock_analytics
        )
        SELECT s.sector, s.industry, s.market_cap, la.perf_1w, la.perf_1m, la.perf_3m, la.perf_6m, la.perf_ytd
        FROM stocks s JOIN latest_analytics la ON s.ticker = la.ticker
        WHERE la.rn = 1 AND s.sector != 'Unknown' AND s.sector IS NOT NULL;
        """
        heatmap_df = pd.read_sql_query(heatmap_query, con=engine)
        logging.info(f"✅ Loaded heatmap data for {len(heatmap_df)} stocks.")

        if not heatmap_df.empty:
            industry_sector_pairs = heatmap_df[['industry', 'sector']].drop_duplicates()
            industry_to_sector_map = pd.Series(industry_sector_pairs.sector.values, index=industry_sector_pairs.industry).to_dict()
            logging.info(f"✅ Created map for {len(industry_to_sector_map)} industries to sectors.")

        # --- PART F: Load Data for Group Analysis ---
        group_analytics_df = pd.read_sql_query("SELECT * FROM daily_group_analytics", con=engine)
        group_analytics_df['analysis_date'] = pd.to_datetime(group_analytics_df['analysis_date'])
        logging.info(f"✅ Loaded {len(group_analytics_df)} rows of group analytics data.")
        
        # --- PART G: Load LATEST data for ALL individual stocks for drill-down ---
        stock_list_query = """
        WITH latest_data AS (
            SELECT *, ROW_NUMBER() OVER(PARTITION BY ticker ORDER BY date DESC) as rn
            FROM daily_stock_analytics
        )
        SELECT s.ticker, s.sector, s.industry, ld.rs, ld.trend, ld.perf_1m, ld.perf_ytd
        FROM stocks s JOIN latest_data ld ON s.ticker = ld.ticker WHERE ld.rn = 1;
        """
        latest_stock_analytics_df = pd.read_sql_query(stock_list_query, con=engine)
        logging.info(f"✅ Loaded latest analytics for {len(latest_stock_analytics_df)} individual stocks.")
        
        # --- PART H: Load Data for Breakout Modal ---
        breakout_stocks_df = pd.read_sql_query("SELECT * FROM daily_breakout_stocks", con=engine, parse_dates=['date'])
        logging.info(f"✅ Loaded {len(breakout_stocks_df)} breakout stock instances.")
        
        # --- PART I: Load Stock Metadata for Modal Formatting ---
        stock_metadata_df = pd.read_sql_query("SELECT ticker, sector, industry FROM stocks", con=engine)
        logging.info(f"✅ Loaded stock metadata for {len(stock_metadata_df)} tickers.")
        
        logging.info("✅ Data module initialized successfully.")
    except Exception as e:
        logging.error(f"❌ A major error occurred during data loading: {e}", exc_info=True)