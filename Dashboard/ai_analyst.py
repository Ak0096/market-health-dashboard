# --- START OF COMPLETE, REFACTORED ai_analyst.py ---

import os
import google.generativeai as genai
import pandas as pd
import logging
from dotenv import load_dotenv

load_dotenv()

def get_trend_word(change, positive_is_good=True):
    """Helper function to describe a trend."""
    if pd.isna(change):
        return "Stable"
    if abs(change) < 0.1: # Use a small threshold to avoid noise
        return "Stable"
    
    if positive_is_good:
        return "Improving" if change > 0 else "Worsening"
    else: # For metrics like VIX where lower is better
        return "Improving" if change < 0 else "Worsening"


def generate_market_summary(breadth_for_ai: pd.DataFrame, indicators_df: pd.DataFrame) -> str:
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        logging.error("❌ Gemini API key not found in environment variables. Please set GEMINI_API_KEY.")
        return "Error: The `GEMINI_API_KEY` environment variable is not set."

    try:
        genai.configure(api_key=api_key) # type: ignore
        model = genai.GenerativeModel('gemini-2.5-pro') # type: ignore

        if len(breadth_for_ai) < 6 or len(indicators_df) < 6:
            return "Error: Not enough historical data to generate a trend analysis (need at least 6 days)."

        latest_breadth = breadth_for_ai.iloc[-1]
        prev_breadth = breadth_for_ai.iloc[0]
        
        latest_indicators = indicators_df.iloc[-1]
        prev_indicators = indicators_df.iloc[-6]

        breadth_200_change = latest_breadth['pct_above_200'] - prev_breadth['pct_above_200']
        
        # --- THIS IS THE FIX: Explicitly handle potentially missing macro data ---
        vix_val = latest_indicators.get('vixcls')
        yield_spread_val = latest_indicators.get('t10y2y')
        
        vix_change = vix_val - prev_indicators.get('vixcls') if pd.notna(vix_val) and pd.notna(prev_indicators.get('vixcls')) else None
        yield_spread_change = yield_spread_val - prev_indicators.get('t10y2y') if pd.notna(yield_spread_val) and pd.notna(prev_indicators.get('t10y2y')) else None
        
        latest_date = latest_breadth.name
        date_str = latest_date.strftime('%Y-%m-%d') if isinstance(latest_date, pd.Timestamp) else str(latest_date)

        data_context = f"""
        **Market Health Indicators for {date_str}:**
        - **Market Breadth (% > 200D MA):** Current value is {latest_breadth['pct_above_200']:.1f}%. The 1-week trend is **{get_trend_word(breadth_200_change, positive_is_good=True)}**.
        - **Economic Outlook (10Y-2Y Spread):** Current value is {f"{yield_spread_val:.2f}" if pd.notna(yield_spread_val) else "Unavailable"}. The 1-week trend is **{get_trend_word(yield_spread_change, positive_is_good=True)}**.
        - **Market Fear (VIX):** Current value is {f"{vix_val:.2f}" if pd.notna(vix_val) else "Unavailable"}. The 1-week trend is **{get_trend_word(vix_change, positive_is_good=False)}**.
        - **Buying Conviction (Breakouts):** Today saw {latest_indicators.get('high_volume_breakout_count', 0)} high-volume breakouts.
        """
        # --- END OF FIX ---

        prompt = f"""
        **Your Role:** You are a sharp, no-fluff market strategist.
        **Your Task:** Synthesize the provided market health indicators into a concise summary.
        **Instructions:**
        1.  Begin with a one-sentence "Bottom Line" conclusion.
        2.  Analyze both the current value and the 1-week trend for each indicator.
        3.  If a key indicator's value is "Unavailable", you MUST state this clearly and explain that it creates a blind spot in the analysis. This is a critical risk to highlight.
        4.  Synthesize the available data. Do not just list it. Explain what the combination of indicators implies for overall market health.
        5.  Keep the entire summary to three short paragraphs.

        **Data to Analyze:**
        {data_context}

        **Begin Analysis:**
        """

        response = model.generate_content(prompt)
        return response.text

    except Exception as e:
        logging.error(f"❌ An error occurred with the Gemini API: {e}")
        return f"An error occurred while communicating with the AI model: {str(e)}"