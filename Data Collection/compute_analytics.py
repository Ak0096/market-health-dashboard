import json
import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
import logging
import time
import io
import os
import csv
from datetime import timedelta
from typing import Dict, List, Any, Tuple

def setup_logging():
    """Configures a logger for the script."""
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    if logger.hasHandlers():
        logger.handlers.clear()
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)
    fh = logging.FileHandler('compute_analytics.log', mode='w', encoding='utf-8')
    fh.setLevel(logging.INFO)
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    return logger

def load_config(logger, default_config_path='config.json'):
    """Loads the configuration file using a robust path relative to the script itself."""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(script_dir, default_config_path)
    
    logger.info(f"Loading configuration from {config_path}...")
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
        logger.info("✅ Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logger.critical(f"❌ FATAL ERROR: config.json not found at the expected path: {config_path}")
        return None
    except Exception as e:
        logger.critical(f"❌ FATAL ERROR loading config: {e}")
        return None

def get_calculation_date_range(engine, logger):
    """Finds the date from which analytics need to be (re)calculated."""
    with engine.connect() as connection:
        last_analytics_date = connection.execute(text("SELECT MAX(date) FROM daily_stock_analytics")).scalar()
        
        # Buffer to ensure enough historical data for MA-200 calculation
        buffer_days = 300 
        
        if last_analytics_date:
            recalc_from_date = last_analytics_date - timedelta(days=252) # Recalculate last year for safety
            data_load_date = recalc_from_date - timedelta(days=buffer_days)
            logger.info(f"Last analytics on {last_analytics_date}. Loading raw data from {data_load_date} to recalculate from {recalc_from_date}.")
            return (data_load_date, recalc_from_date)
        else:
            logger.info("No existing analytics found. Querying for the first raw data date...")
            first_raw_date = connection.execute(text("SELECT MIN(date) FROM daily_stock_data")).scalar()
            if first_raw_date:
                logger.info("Calculating from the beginning of all raw data.")
                return (first_raw_date, first_raw_date)
            else:
                logger.error("CRITICAL: No raw data found in daily_stock_data table. Cannot proceed.")
                return None

def calculate_indicators(df: pd.DataFrame, logger: logging.Logger, calc_start_date=None) -> pd.DataFrame:
    """Calculates all stock-specific analytical indicators."""
    logger.info("--- Starting Stock-Specific Indicator Calculations ---")
    df['date'] = pd.to_datetime(df['date'])
    
    logger.info("Step 1/6: Calculating HLCC4...")
    df['close_safe'] = df['close'].replace(0, np.nan)
    df['adjustment_factor'] = df['adj_close'] / df['close_safe']
    df['adjustment_factor'] = df['adjustment_factor'].fillna(1.0)
    df['adj_high'] = df['high'] * df['adjustment_factor']
    df['adj_low'] = df['low'] * df['adjustment_factor']
    df['hlcc4'] = (df['adj_high'] + df['adj_low'] + df['adj_close'] + df['adj_close']) / 4
    df.drop(columns=['close_safe'], inplace=True)
    
    logger.info("Step 2/6: Calculating Moving Averages...")
    # Group by ticker before applying rolling calculations
    grouped = df.groupby('ticker')
    df['ma_20'] = grouped['hlcc4'].transform(lambda x: x.rolling(window=20, min_periods=20).mean())
    df['ma_50'] = grouped['hlcc4'].transform(lambda x: x.rolling(window=50, min_periods=50).mean())
    df['ma_200'] = grouped['hlcc4'].transform(lambda x: x.rolling(window=200, min_periods=200).mean())
    
    logger.info("Step 3/6: Calculating Relative Strength...")
    spx_df = df[df['ticker'] == '^GSPC'][['date', 'hlcc4']].rename(columns={'hlcc4': 'spx_hlcc4'})
    df = pd.merge(df, spx_df, on='date', how='left')
    
    # --- THIS IS THE FIX ---
    # Replace infinite values that can result from division by zero with NaN
    df['rs'] = df['hlcc4'] / df['spx_hlcc4']
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    
    # Now, drop any row where RS could not be calculated. This is the critical hardening step.
    # If RS is NaN, the entire row of analytics for that day is invalid.
    initial_rows = len(df)
    df.dropna(subset=['rs'], inplace=True)
    rows_dropped = initial_rows - len(df)
    if rows_dropped > 0:
        logger.warning(f"Dropped {rows_dropped} rows due to invalid RS calculation (likely missing S&P 500 data).")
    # --- END OF FIX ---
    
    logger.info("Step 4/6: Determining Trend State...")
    uptrend_conditions = (df['adj_close'] > df['ma_50']) & (df['ma_50'] > df['ma_200'])
    downtrend_conditions = (df['adj_close'] < df['ma_50']) & (df['ma_50'] < df['ma_200'])
    df['trend'] = np.select([uptrend_conditions, downtrend_conditions], ['Uptrend', 'Downtrend'], default='Sideways')
    
    logger.info("Step 5/6: Calculating Performance Metrics...")
    # Re-group after the merge and dropna
    grouped_adj_close = df.groupby('ticker')['adj_close']
    df['perf_1w'] = grouped_adj_close.pct_change(periods=5) * 100
    df['perf_1m'] = grouped_adj_close.pct_change(periods=21) * 100
    df['perf_3m'] = grouped_adj_close.pct_change(periods=63) * 100
    df['perf_6m'] = grouped_adj_close.pct_change(periods=126) * 100
    df['year'] = df['date'].dt.year
    df['ytd_start_price'] = df.groupby(['ticker', 'year'])['adj_close'].transform('first')
    df['perf_ytd'] = (df['adj_close'] / df['ytd_start_price'] - 1) * 100
    
    logger.info("Step 6/6: Preparing final data for storage...")
    final_cols = ['ticker', 'date', 'hlcc4', 'ma_20', 'ma_50', 'ma_200', 'rs', 'trend', 
                  'perf_1w', 'perf_1m', 'perf_3m', 'perf_6m', 'perf_ytd']
    analytics_df = df[final_cols].copy()
    analytics_df = analytics_df.dropna(subset=['ma_200'])
    
    if calc_start_date:
        analytics_df = analytics_df[analytics_df['date'] >= pd.to_datetime(calc_start_date)]
    
    analytics_df = analytics_df.round(4)
    logger.info(f"✅ Stock-specific analytics prepared. {len(analytics_df)} valid rows calculated.")
    return analytics_df

def calculate_group_analytics(daily_analytics_df: pd.DataFrame, stocks_df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    logger.info("--- Starting Group-Level Analytics Calculation ---")
    if daily_analytics_df.empty:
        logger.warning("⚠️ No analytics data for group calculations. Skipping.")
        return pd.DataFrame()
    
    merged_df = pd.merge(daily_analytics_df, stocks_df, on='ticker')
    if merged_df.empty:
        logger.warning("⚠️ No data after merging with stocks metadata. Skipping.")
        return pd.DataFrame()
    
    all_group_analytics = []
    for group_type in ['sector', 'industry']:
        logger.info(f"Processing groups of type: '{group_type}'...")
        filtered_groups = merged_df[merged_df[group_type] != 'Unknown']
        if filtered_groups.empty: continue
        
        group_rs_numerator = filtered_groups.groupby(['date', group_type]).apply(lambda x: (x['rs'] * x['market_cap']).sum(), include_groups=False)
        group_rs_denominator = filtered_groups.groupby(['date', group_type])['market_cap'].sum()
        if group_rs_numerator.empty or group_rs_denominator.empty: continue
            
        group_rs_weighted = (group_rs_numerator / group_rs_denominator).rename('group_rs_value')
        group_df = pd.DataFrame(group_rs_weighted).reset_index()
        
        grouped = group_df.groupby(group_type)['group_rs_value']
        group_df['group_rs_sma_20'] = grouped.transform(lambda x: x.rolling(window=20, min_periods=20).mean())
        group_df['group_rs_sma_50'] = grouped.transform(lambda x: x.rolling(window=50, min_periods=50).mean())
        group_df['group_rs_sma_200'] = grouped.transform(lambda x: x.rolling(window=200, min_periods=200).mean())
        
        group_df.sort_values(by=[group_type, 'date'], inplace=True)
        roc_period = 20
        group_df['group_rs_roc_20'] = group_df.groupby(group_type)['group_rs_value'].transform(lambda x: ((x - x.shift(roc_period)) / x.shift(roc_period)) * 100)
        
        group_df['above_rs_20sma'] = group_df['group_rs_value'] > group_df['group_rs_sma_20']
        group_df['above_rs_50sma'] = group_df['group_rs_value'] > group_df['group_rs_sma_50']
        group_df['above_rs_200sma'] = group_df['group_rs_value'] > group_df['group_rs_sma_200']
        
        group_df['group_type'] = group_type
        group_df.rename(columns={group_type: 'group_name', 'date': 'analysis_date'}, inplace=True)
        all_group_analytics.append(group_df)
    
    if not all_group_analytics: return pd.DataFrame()
    
    final_df = pd.concat(all_group_analytics, ignore_index=True)
    db_columns = ['analysis_date', 'group_name', 'group_type', 'group_rs_value', 'group_rs_sma_20', 'group_rs_sma_50', 'group_rs_sma_200', 'above_rs_20sma', 'above_rs_50sma', 'above_rs_200sma', 'group_rs_roc_20']
    final_df = final_df[db_columns].dropna(subset=['group_rs_sma_200']).round(4)
    logger.info(f"✅ Group-level analytics calculated. {len(final_df)} rows generated.")
    return final_df

def calculate_market_breadth(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    logger.info("--- Calculating Market Breadth (Advance/Decline Line) ---")
    df = df.copy()
    df['adj_close'] = pd.to_numeric(df['adj_close'], errors='coerce')
    df_pivot = df.pivot_table(index='date', columns='ticker', values='adj_close')
    price_change = df_pivot.drop(columns=['^GSPC'], errors='ignore').diff()
    advancers = (price_change > 0).sum(axis=1)
    decliners = (price_change < 0).sum(axis=1)
    breadth_df = pd.DataFrame({'advancers': advancers, 'decliners': decliners})
    breadth_df['ad_line'] = (breadth_df['advancers'] - breadth_df['decliners']).cumsum()
    logger.info("✅ Market Breadth calculated.")
    return breadth_df.reset_index()

def calculate_breakouts_and_volume(df: pd.DataFrame, logger: logging.Logger):
    logger.info("--- Calculating Volume Breakouts and Stats ---")
    df['date'] = pd.to_datetime(df['date'])
    df.sort_values(by=['ticker', 'date'], inplace=True)
    grouped = df.groupby('ticker')
    
    df['vol_ma_50'] = grouped['volume'].transform(lambda x: x.rolling(window=50, min_periods=50).mean())
    df['close_rolling_high_20'] = grouped['close'].transform(lambda x: x.rolling(window=20, min_periods=20).max())
    is_new_high = df['close'] >= df['close_rolling_high_20'].shift(1)
    is_high_volume = df['volume'] > (df['vol_ma_50'] * 1.5)
    df['is_breakout_stock'] = is_new_high & is_high_volume
    df['is_above_avg_vol'] = df['volume'] > df['vol_ma_50']
    
    daily_summary = df.groupby('date').agg(
        total_stocks=('ticker', 'count'),
        high_volume_breakout_count=('is_breakout_stock', 'sum'),
        above_avg_vol_count=('is_above_avg_vol', 'sum')
    )
    daily_summary['pct_above_avg_volume'] = (daily_summary['above_avg_vol_count'] / daily_summary['total_stocks']) * 100
    breakout_stats_df = daily_summary[['high_volume_breakout_count', 'pct_above_avg_volume']].round(2).dropna()
    breakout_stocks_df = df[df['is_breakout_stock']][['date', 'ticker']].copy()
    logger.info(f"✅ Breakout analysis complete.")
    return breakout_stats_df.reset_index(), breakout_stocks_df

def main():
    start_time = time.time()
    logger = setup_logging()
    
    config = load_config(logger)
    if not config: return
        
    try:
        db_config = config['database']
        engine_url = f"postgresql+psycopg2://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['dbname']}"
        engine = create_engine(engine_url)
        with engine.connect() as connection: connection.execute(text("SELECT 1"))
        logger.info("Database connection successful.")
    except Exception as e:
        logger.critical(f"❌ FATAL ERROR creating database engine: {e}")
        return
    
    with engine.connect() as connection:
        refreshed_tickers_result = connection.execute(text("SELECT ticker FROM refreshed_tickers_log")).fetchall()
        tickers_to_fully_recalculate = {row[0] for row in refreshed_tickers_result}
        logger.info(f"Found {len(tickers_to_fully_recalculate)} tickers from log requiring full analytics recalculation.")
    
    date_range_result = get_calculation_date_range(engine, logger)
    if not date_range_result:
        logger.warning("No data to process. Exiting.")
        return
    
    data_load_date, recalc_from_date = date_range_result
    data_load_date_sql = data_load_date.strftime('%Y-%m-%d')
    recalc_from_date_sql = recalc_from_date.strftime('%Y-%m-%d')
    
    logger.info("--- Loading Required Raw Data Slice ---")
    
    # --- THIS IS THE DEFINITIVE FIX ---
    
    # 1. Load the full history for the S&P 500. This is our universal benchmark.
    spx_query = text("SELECT ticker, date, high, low, close, adj_close, volume FROM daily_stock_data WHERE ticker = '^GSPC'")
    spx_df = pd.read_sql(spx_query, con=engine, parse_dates=['date'])
    logger.info(f"Loaded full history for S&P 500 benchmark ({len(spx_df)} rows).")

    # 2. Load the incremental slice for all other stocks.
    full_recalc_list = list(tickers_to_fully_recalculate) if tickers_to_fully_recalculate else [None]
    other_stocks_query = text("""
        SELECT ticker, date, high, low, close, adj_close, volume 
        FROM daily_stock_data 
        WHERE ticker != '^GSPC' AND (ticker = ANY(:full_recalc_tickers) OR date >= :start_date)
    """)
    other_stocks_df = pd.read_sql(
        other_stocks_query, 
        con=engine, 
        params={'full_recalc_tickers': full_recalc_list, 'start_date': data_load_date_sql}, 
        parse_dates=['date']
    )
    logger.info(f"Loaded incremental data for {other_stocks_df['ticker'].nunique()} other stocks.")

    # 3. Combine them into a single DataFrame for processing.
    raw_data_df = pd.concat([spx_df, other_stocks_df], ignore_index=True)
    logger.info(f"Total raw data rows to process: {len(raw_data_df)}")
    
    # --- END OF DEFINITIVE FIX ---
    
    stocks_df = pd.read_sql("SELECT ticker, sector, industry, market_cap FROM stocks", con=engine)
    macro_df = pd.read_sql(text("SELECT date, series_id, value FROM macro_data WHERE date >= :start_date"), con=engine, params={'start_date': recalc_from_date_sql}, parse_dates=['date'])
    
    logger.warning(f"Performing targeted deletion of analytics...")
    with engine.connect() as connection:
        with connection.begin():
            if tickers_to_fully_recalculate:
                logger.info(f"Deleting full analytics history for {len(tickers_to_fully_recalculate)} refreshed tickers.")
                connection.execute(text("DELETE FROM daily_stock_analytics WHERE ticker = ANY(:tickers)"), {'tickers': list(tickers_to_fully_recalculate)})
                connection.execute(text("DELETE FROM daily_breakout_stocks WHERE ticker = ANY(:tickers)"), {'tickers': list(tickers_to_fully_recalculate)})

            logger.info(f"Deleting recent analytics for all tickers from {recalc_from_date_sql} onwards.")
            connection.execute(text("DELETE FROM daily_stock_analytics WHERE date >= :start_date"), {'start_date': recalc_from_date_sql})
            connection.execute(text("DELETE FROM daily_market_indicators WHERE date >= :start_date"), {'start_date': recalc_from_date_sql})
            connection.execute(text("DELETE FROM daily_breakout_stocks WHERE date >= :start_date"), {'start_date': recalc_from_date_sql})
            
            logger.info("Truncating group analytics to prepare for a full, consistent rebuild.")
            connection.execute(text("TRUNCATE TABLE daily_group_analytics;"))
            
            connection.execute(text("TRUNCATE TABLE refreshed_tickers_log;"))
    
    newly_calculated_analytics_df = calculate_indicators(raw_data_df.copy(), logger, recalc_from_date)
    
    logger.info("Loading all stock analytics to rebuild group data...")
    historical_analytics_df = pd.read_sql("SELECT * FROM daily_stock_analytics", con=engine, parse_dates=['date'])
    full_analytics_df = pd.concat([historical_analytics_df, newly_calculated_analytics_df]).drop_duplicates(subset=['ticker', 'date'], keep='last')
    group_analytics_df = calculate_group_analytics(full_analytics_df, stocks_df, logger)
    
    raw_data_for_market = raw_data_df[raw_data_df['date'] >= pd.to_datetime(recalc_from_date)]
    breakout_stats_df, breakout_stocks_df = calculate_breakouts_and_volume(raw_data_for_market.copy(), logger)
    market_breadth_df = calculate_market_breadth(raw_data_for_market, logger)
    
    logger.info("--- Saving results to database ---")
    
    if not newly_calculated_analytics_df.empty:
        newly_calculated_analytics_df.to_sql('daily_stock_analytics', engine, if_exists='append', index=False)
    
    if not market_breadth_df.empty:
        macro_pivot_df = macro_df.pivot(index='date', columns='series_id', values='value') if not macro_df.empty else pd.DataFrame()
        market_indicators_df = market_breadth_df.set_index('date')
        if not breakout_stats_df.empty:
            market_indicators_df = market_indicators_df.join(breakout_stats_df.set_index('date'))
        if not macro_pivot_df.empty:
            market_indicators_df = pd.merge_asof(
                left=market_indicators_df.sort_index(), right=macro_pivot_df.sort_index(),
                left_index=True, right_index=True, direction='forward'
            )
        market_indicators_df.reset_index(inplace=True)
        market_indicators_df.rename(columns=lambda c: c.lower(), inplace=True)
        market_indicators_df.to_sql('daily_market_indicators', engine, if_exists='append', index=False)
    
    if not breakout_stocks_df.empty:
        breakout_stocks_df.to_sql('daily_breakout_stocks', engine, if_exists='append', index=False)
    
    if not group_analytics_df.empty:
        group_analytics_df.to_sql('daily_group_analytics', engine, if_exists='append', index=False)
    
    end_time = time.time()
    logger.info(f"✅ Analytics Pipeline Completed in {end_time - start_time:.2f} seconds")

if __name__ == "__main__":
    main()