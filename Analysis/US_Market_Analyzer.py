import pandas as pd
import numpy as np
import json
import logging
import os
from datetime import datetime
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from typing import Optional, Dict, Any
from tqdm import tqdm

# === Logging Setup ===
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class USMarketAnalyzer:
    """
    An optimized script to perform Relative Strength (RS) analysis on a universe of stocks
    from a PostgreSQL database, using a fully vectorized approach for maximum performance.
    """
    def __init__(self, config_path: str = "Data Collection/config.json"):
        self.config: Optional[Dict[str, Any]] = self.load_config(config_path)
        self.engine: Optional[Engine] = None
        
        if not self.config:
            logging.critical("❌ Critical: Failed to load configuration file. Initialization failed.")
            return

        self.setup_database_connection()

    def load_config(self, path: str) -> Optional[Dict[str, Any]]:
        """Load a JSON configuration file."""
        if not os.path.exists(path):
            logging.error(f"Config file not found: {path}")
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except json.JSONDecodeError:
            logging.error(f"Error decoding JSON from config file: {path}")
            return None

    def setup_database_connection(self) -> None:
        """Safely establishes a connection to the PostgreSQL database."""
        # Add explicit check to satisfy type checker
        if not self.config:
            logging.error("❌ Configuration not loaded.")
            self.engine = None
            return
            
        db_conf = self.config.get('database')
        if not db_conf:
            logging.error("❌ 'database' section not found in config file.")
            self.engine = None
            return
            
        try:
            connection_string = f"postgresql://{db_conf['user']}:{db_conf['password']}@{db_conf['host']}:{db_conf['port']}/{db_conf['dbname']}"
            self.engine = create_engine(connection_string)
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logging.info("✅ Database connection established successfully.")
        except KeyError as e:
            logging.error(f"❌ Missing required key in 'database' config section: {e}")
            self.engine = None
        except Exception as e:
            logging.error(f"❌ Database connection failed: {e}")
            self.engine = None

    def load_data_from_database(self) -> Optional[pd.DataFrame]:
        """Loads all required data for all stocks using a single, efficient JOIN query."""
        if not self.engine:
            logging.error("Cannot load data, no database engine.")
            return None
            
        logging.info("Executing master query to load all data from the database...")
        query = text("""
            SELECT s.ticker, s.sector, s.industry, d.date, d.close, d.volume,
                   a.hlcc4, a.ma_20, a.ma_50, a.ma_200
            FROM stocks s
            JOIN daily_stock_data d ON s.ticker = d.ticker
            JOIN daily_stock_analytics a ON s.ticker = a.ticker AND d.date = a.date
            ORDER BY s.ticker, d.date;
        """)
        
        try:
            with self.engine.connect() as conn:
                df = pd.read_sql(query, conn, parse_dates=['date'])
            
            if df.empty:
                logging.error("❌ No data returned from the database. Please check if the tables are populated.")
                return None
            
            logging.info(f"✅ Successfully loaded {len(df)} total data points for {df['ticker'].nunique()} unique tickers.")
            return df
        except Exception as e:
            logging.error(f"❌ Failed to execute database query: {e}")
            return None

    def calculate_rs_trend(self, crs_weekly: pd.Series) -> str:
        """Calculate RS trend based on 12-week WoW changes."""
        if len(crs_weekly) < 13:
            return "Insufficient Data"
        
        recent_12_weeks_change = crs_weekly.pct_change().dropna().tail(12)
        positive_weeks = (recent_12_weeks_change > 0).sum()
        
        if positive_weeks >= 8: return "Uptrend"
        if positive_weeks <= 4: return "Downtrend"
        return "Sideways"

    def run_vectorized_analysis(self, df: pd.DataFrame) -> Optional[pd.DataFrame]:
        """Performs the entire RS analysis using vectorized pandas operations."""
        if not self.config: return None
        logging.info("Starting vectorized analysis...")

        benchmark_ticker = self.config.get('yfinance', {}).get('benchmark_ticker', '^GSPC')
        if not benchmark_ticker:
            logging.error("❌ Benchmark ticker is not defined in config.json under the 'yfinance' section.")
            return None
        
        if benchmark_ticker not in df['ticker'].unique():
            logging.error(f"❌ Benchmark ticker '{benchmark_ticker}' not found in the loaded data.")
            return None
        
        benchmark_df = df[df['ticker'] == benchmark_ticker][['date', 'hlcc4']].rename(
            columns={'hlcc4': 'hlcc_index'}
        ).copy()

        df = pd.merge(df, benchmark_df, on='date', how='left')
        df['hlcc_index'] = df['hlcc_index'].replace(0, np.nan)
        df.dropna(subset=['hlcc_index'], inplace=True)
        df['crs'] = df['hlcc4'] / df['hlcc_index']
        logging.info("Calculated daily Comparative Relative Strength (CRS).")

        all_tickers = df['ticker'].unique()
        weekly_results = []
        logging.info("Resampling daily data to weekly frequency...")
        for ticker in tqdm(all_tickers, desc="Resampling Stocks"):
            ticker_df = df[df['ticker'] == ticker]
            weekly_agg = ticker_df.resample('W-MON', on='date').agg({
                'crs': 'last', 'volume': 'sum'
            }).reset_index()
            weekly_agg['ticker'] = ticker
            weekly_results.append(weekly_agg)
        weekly_df = pd.concat(weekly_results, ignore_index=True)

        weekly_df.sort_values(by=['ticker', 'date'], inplace=True)
        weekly_df['rs_3m_change'] = weekly_df.groupby('ticker')['crs'].pct_change(periods=13) * 100
        weekly_df['rs_6m_change'] = weekly_df.groupby('ticker')['crs'].pct_change(periods=26) * 100
        weekly_df['rs_12m_change'] = weekly_df.groupby('ticker')['crs'].pct_change(periods=52) * 100
        weekly_df['avg_vol_9w'] = weekly_df.groupby('ticker')['volume'].transform(lambda x: x.rolling(window=9).mean())
        weekly_df['volume_strength'] = weekly_df['volume'] / weekly_df['avg_vol_9w'].replace(0, np.nan)
        weekly_df['rs_trend'] = weekly_df.groupby('ticker')['crs'].transform(self.calculate_rs_trend)
        logging.info("Calculated all weekly analytics.")

        latest_daily_data = df.loc[df.groupby('ticker')['date'].idxmax()].set_index('ticker')
        latest_weekly_data = weekly_df.loc[weekly_df.groupby('ticker')['date'].idxmax()].set_index('ticker')

        summary_df = latest_daily_data.join(latest_weekly_data, rsuffix='_weekly')
        final_cols = {
            'sector': 'sector', 'industry': 'industry', 'close': 'latest_close',
            'ma_20': 'sma_20', 'ma_50': 'sma_50', 'ma_200': 'sma_200',
            'rs_3m_change': 'rs_3m_change', 'rs_6m_change': 'rs_6m_change',
            'rs_12m_change': 'rs_12m_change', 'rs_trend': 'rs_trend',
            'volume_strength': 'volume_strength', 'crs': 'crs_weekly_current'
        }
        summary_df = summary_df.rename(columns=final_cols)
        summary_df['above_20_ma'] = summary_df['latest_close'] > summary_df['sma_20']
        summary_df['above_50_ma'] = summary_df['latest_close'] > summary_df['sma_50']
        summary_df['above_200_ma'] = summary_df['latest_close'] > summary_df['sma_200']
        summary_df['symbol'] = summary_df.index
        summary_df['data_start_date'] = weekly_df.groupby('ticker')['date'].min()
        summary_df['latest_date'] = weekly_df.groupby('ticker')['date'].max()
        summary_df['data_days_available'] = (summary_df['latest_date'] - summary_df['data_start_date']).dt.days

        logging.info("✅ Vectorized analysis complete.")
        return summary_df.reset_index(drop=True)

    def calculate_momentum_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """Calculate Z-scores and momentum scores for the analyzed stocks."""
        if df.empty: return pd.DataFrame()
        score_cols = ['rs_3m_change', 'rs_6m_change', 'rs_12m_change', 'volume_strength']
        for col in score_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

        def z_score(series: pd.Series) -> pd.Series:
            std_dev = series.std(ddof=0)
            if std_dev != 0:
                return (series - series.mean()) / std_dev
            else:
                return pd.Series(0, index=series.index, dtype=float)

        for col in ['rs_3m_change', 'rs_6m_change', 'rs_12m_change', 'volume_strength']:
            df[f'z_{col.replace("_change", "")}'] = z_score(df[col])
            
        df['momentum_score_z'] = (0.5 * df['z_rs_3m'] + 0.3 * df['z_rs_6m'] + 0.2 * df['z_rs_12m'])
        df['rank_position'] = df['momentum_score_z'].rank(ascending=False, method='min').astype(int)
        df['category'] = pd.cut(df['rank_position'], bins=[0, 20, 50, np.inf], labels=['Top Pick', 'Watchlist', 'Lagging'], right=True)
        
        return df

    def export_to_excel(self, summary_df: pd.DataFrame, output_file: str) -> bool:
        """Export the full analysis to a multi-sheet Excel file."""
        if summary_df.empty:
            logging.warning("No data to export to Excel.")
            return False
        logging.info(f"Exporting comprehensive results to {output_file}...")
        try:
            with pd.ExcelWriter(output_file, engine='xlsxwriter') as writer:
                export_cols = [
                    'rank_position', 'symbol', 'sector', 'industry', 'latest_close', 
                    'rs_3m_change', 'rs_6m_change', 'rs_12m_change', 'rs_trend', 'volume_strength',
                    'momentum_score_z', 'category', 'above_20_ma', 'above_50_ma', 'above_200_ma'
                ]
                export_df = summary_df[[col for col in export_cols if col in summary_df.columns]].sort_values('rank_position').reset_index(drop=True)
                export_df.to_excel(writer, sheet_name="RS_Analysis", index=False)
                
                total_stocks = len(summary_df)
                if total_stocks > 0:
                    breadth_metrics = {
                        'Metric': ['Total Stocks Analyzed', 'Stocks in RS Uptrend', '% Stocks in RS Uptrend', 'Stocks Above 200-Day MA', '% Stocks Above 200-Day MA', 'Stocks Above 50-Day MA', '% Stocks Above 50-Day MA', 'Stocks Above 20-Day MA', '% Stocks Above 20-Day MA', 'Top Pick Count', 'Watchlist Count', 'Lagging Count'],
                        'Value': [total_stocks, (summary_df['rs_trend'] == 'Uptrend').sum(), f"{((summary_df['rs_trend'] == 'Uptrend').sum() / total_stocks * 100):.1f}%", summary_df['above_200_ma'].sum(), f"{(summary_df['above_200_ma'].sum() / total_stocks * 100):.1f}%", summary_df['above_50_ma'].sum(), f"{(summary_df['above_50_ma'].sum() / total_stocks * 100):.1f}%", summary_df['above_20_ma'].sum(), f"{(summary_df['above_20_ma'].sum() / total_stocks * 100):.1f}%", (summary_df['category'] == 'Top Pick').sum(), (summary_df['category'] == 'Watchlist').sum(), (summary_df['category'] == 'Lagging').sum()]
                    }
                    pd.DataFrame(breadth_metrics).to_excel(writer, sheet_name="Market_Breadth", index=False)

                sector_agg = summary_df.groupby('sector').agg(
                    Avg_Momentum_Score=('momentum_score_z', 'mean'), Stock_Count=('symbol', 'count'),
                    Uptrend_Count=('rs_trend', lambda x: (x == 'Uptrend').sum())
                ).round(2).reset_index().sort_values('Avg_Momentum_Score', ascending=False)
                sector_agg.to_excel(writer, sheet_name="Sector_Analysis", index=False)
            
            logging.info(f"✅ Results exported to {output_file}")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to export to Excel: {e}")
            return False

    def run_analysis_pipeline(self, export_excel: bool = True, output_file: str = "us_market_rs_analysis.xlsx") -> bool:
        """Run the new, streamlined, end-to-end analysis pipeline."""
        if not self.engine:
            logging.error("❌ Database connection not established. Cannot run analysis.")
            return False
        
        logging.info("=== 🚀 Starting US Market Vectorized RS Analysis ===")
        
        # --- FIX 2: Correct the typo in the function call ---
        master_df = self.load_data_from_database()
        if master_df is None or master_df.empty: return False
            
        summary_df = self.run_vectorized_analysis(master_df)
        if summary_df is None or summary_df.empty:
            logging.error("❌ Analysis produced no results."); return False

        logging.info("📊 Calculating final momentum scores and rankings...")
        summary_df = self.calculate_momentum_scores(summary_df)
        
        if export_excel:
            self.export_to_excel(summary_df, output_file)

        logging.info("=== ✅ US MARKET ANALYSIS COMPLETE ===")
        return True

def main():
    analyzer = USMarketAnalyzer()
    if not analyzer.engine:
        logging.error("❌ Failed to initialize analyzer. Exiting.")
        return

    # --- FIX 3: Add a guard clause to ensure analyzer.config exists before use ---
    config = analyzer.config
    if not config:
        logging.error("❌ Configuration is not loaded. Cannot determine output settings.")
        return
        
    output_config = config.get('output_settings', {})
    output_dir = output_config.get('reports_directory', 'reports')
    
    if output_config.get('create_dated_folders', True):
        output_dir = os.path.join(output_dir, datetime.today().strftime('%Y-%m-%d'))
    
    os.makedirs(output_dir, exist_ok=True)
    
    filename = f"{output_config.get('filename_prefix', 'us_market_rs')}_{datetime.today().strftime('%Y%m%d')}.xlsx"
    full_path = os.path.join(output_dir, filename)

    logging.info(f"📁 Output will be saved to: {full_path}")
    
    start_time = datetime.now()
    success = analyzer.run_analysis_pipeline(export_excel=True, output_file=full_path)
    end_time = datetime.now()
    
    if success:
        logging.info(f"🎉 Analysis completed successfully in {end_time - start_time}")
    else:
        logging.error("❌ US Market RS analysis failed.")
    
    if analyzer.engine:
        analyzer.engine.dispose()

if __name__ == "__main__":
    main()