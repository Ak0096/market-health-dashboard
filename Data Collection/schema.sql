-- PostgreSQL Schema for the US Market Health Dashboard (Definitive Version)

-- This table holds the master list of stocks in our universe.
CREATE TABLE IF NOT EXISTS stocks (
    ticker VARCHAR(20) PRIMARY KEY,
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap_category VARCHAR(50),
    market_cap BIGINT
);

-- This is the primary data table, holding daily price and volume information.
CREATE TABLE IF NOT EXISTS daily_stock_data (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(20) REFERENCES stocks(ticker) ON DELETE CASCADE,
    date DATE NOT NULL,
    open NUMERIC(10, 2),
    high NUMERIC(10, 2),
    low NUMERIC(10, 2),
    close NUMERIC(10, 2),
    adj_close NUMERIC(10, 2),
    volume BIGINT,
    UNIQUE(ticker, date)
);

CREATE INDEX IF NOT EXISTS idx_daily_stock_data_ticker_date 
ON daily_stock_data(ticker, date);

-- This table stores macroeconomic data series fetched from FRED.
CREATE TABLE IF NOT EXISTS macro_data (
    date DATE NOT NULL,
    series_id VARCHAR(20) NOT NULL,
    value NUMERIC,
    PRIMARY KEY (date, series_id)
);

-- Log table for communicating refreshed tickers between pipelines.
CREATE TABLE IF NOT EXISTS refreshed_tickers_log (
    ticker VARCHAR(20) PRIMARY KEY
);


-- ANALYTICS TABLES (populated by compute_analytics.py)

-- Stores the core calculated indicators for each stock for each day.
CREATE TABLE IF NOT EXISTS daily_stock_analytics (
    ticker VARCHAR(20),
    date DATE,
    hlcc4 NUMERIC,
    ma_20 NUMERIC,
    ma_50 NUMERIC,
    ma_200 NUMERIC,
    rs NUMERIC,
    trend VARCHAR(20),
    perf_1w NUMERIC,
    perf_1m NUMERIC,
    perf_3m NUMERIC,
    perf_6m NUMERIC,
    perf_ytd NUMERIC,
    PRIMARY KEY (ticker, date)
);

-- CONSOLIDATED table for all market-wide indicators.
CREATE TABLE IF NOT EXISTS daily_market_indicators (
    date DATE PRIMARY KEY,
    advancers INT,
    decliners INT,
    ad_line BIGINT,
    high_volume_breakout_count INT,
    pct_above_avg_volume NUMERIC,
    dff NUMERIC,
    dgs10 NUMERIC,
    t10y2y NUMERIC,
    vixcls NUMERIC
);

-- Stores a list of individual stocks that had a high-volume breakout on a given day.
CREATE TABLE IF NOT EXISTS daily_breakout_stocks (
    date DATE,
    ticker VARCHAR(20),
    PRIMARY KEY (date, ticker)
);

-- Stores aggregated analytics for sectors and industries.
CREATE TABLE IF NOT EXISTS daily_group_analytics (
    analysis_date DATE,
    group_name VARCHAR(100),
    group_type VARCHAR(20),
    group_rs_value NUMERIC,
    group_rs_sma_20 NUMERIC,
    group_rs_sma_50 NUMERIC,
    group_rs_sma_200 NUMERIC,
    above_rs_20sma BOOLEAN,
    above_rs_50sma BOOLEAN,
    above_rs_200sma BOOLEAN,
    group_rs_roc_20 NUMERIC,
    PRIMARY KEY (analysis_date, group_name, group_type)
);