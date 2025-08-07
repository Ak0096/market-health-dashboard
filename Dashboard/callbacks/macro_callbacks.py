from dash import Input, Output, callback, ctx, dcc, html, State
from dash.exceptions import PreventUpdate
import pandas as pd
from datetime import timedelta
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from dash_bootstrap_templates import ThemeSwitchAIO
from Dashboard.data import latest_date
import dash_bootstrap_components as dbc
import logging

from Dashboard.app import app
# REFACTORED IMPORTS: Removed breadth_df/trend_pct_df, added engine
from Dashboard.data import (
    engine, market_indicators_df, spx_df, latest_df, total_stocks_latest,
    breakout_stocks_df, stock_metadata_df
)
from Dashboard.ai_analyst import generate_market_summary

# --- Gauge and Modal Callbacks (No changes) ---
def create_themed_gauge(value, template):
    fig = go.Figure(go.Indicator(
        mode="gauge+number", value=round(value, 1),
        number={'suffix': "%", 'font': {'size': 50}},
        gauge={'axis': {'range': [0, 100]}, 'bar': {'color': 'lightgray', 'thickness': 0.3},
               'steps': [{'range': [0, 30], 'color': '#d62728'}, {'range': [30, 70], 'color': '#ff7f0e'}, {'range': [70, 100], 'color': '#2ca02c'}]}
    ))
    fig.update_layout(height=250, margin=dict(t=50, b=10, l=10, r=10), template=template)
    return fig

@callback(
    Output('gauge-ma20', 'figure'),
    Output('gauge-ma50', 'figure'),
    Output('gauge-ma200', 'figure'),
    Input(ThemeSwitchAIO.ids.switch("theme"), "value")
)
def update_gauges(toggle_on):
    template = "plotly_dark" if toggle_on else "plotly_white"
    if total_stocks_latest > 0:
        pct_above_ma20 = 100 * (latest_df['adj_close'] > latest_df['ma_20']).sum() / total_stocks_latest
        pct_above_ma50 = 100 * (latest_df['adj_close'] > latest_df['ma_50']).sum() / total_stocks_latest
        pct_above_ma200 = 100 * (latest_df['adj_close'] > latest_df['ma_200']).sum() / total_stocks_latest
    else:
        pct_above_ma20 = pct_above_ma50 = pct_above_ma200 = 0
    return create_themed_gauge(pct_above_ma20, template), create_themed_gauge(pct_above_ma50, template), create_themed_gauge(pct_above_ma200, template)

@callback(
    Output("breakout-modal", "is_open"),
    Output("breakout-modal-title", "children"),
    Output("breakout-modal-body", "children"),
    Input("breakout-chart", "clickData"),
    Input("breakout-modal-close-btn", "n_clicks"),
    State("breakout-modal", "is_open"),
    prevent_initial_call=True
)
def toggle_breakout_modal(clickData, close_clicks, is_open):
    triggered_id = ctx.triggered_id
    if triggered_id == 'breakout-chart' and clickData:
        clicked_date_str = clickData["points"][0]["x"]
        clicked_date = pd.to_datetime(clicked_date_str).date()
        stocks_on_day = breakout_stocks_df[breakout_stocks_df['date'].dt.date == clicked_date]
        title = f"Breakout Stocks for {clicked_date.strftime('%Y-%m-%d')}"
        if stocks_on_day.empty:
            body = html.P("No specific breakout stocks recorded for this day.")
        else:
            merged_df = pd.merge(stocks_on_day, stock_metadata_df, on='ticker')
            grouped_by_sector = merged_df.groupby('sector')
            total_stocks, total_sectors, total_industries = len(merged_df), len(grouped_by_sector), merged_df['industry'].nunique()
            summary = html.P(f"Total: {total_stocks} stocks across {total_sectors} sectors and {total_industries} industries.", className="mb-4")
            sector_tables = []
            sorted_sectors = sorted(grouped_by_sector, key=lambda x: len(x[1]), reverse=True)
            for sector_name, sector_df in sorted_sectors:
                sector_header = html.H4(f"{sector_name} ({len(sector_df)} stocks)", className="mt-4")
                table_header = [html.Thead(html.Tr([html.Th("Ticker"), html.Th("Industry")]))]
                table_rows = [html.Tr([html.Td(row['ticker']), html.Td(row['industry'])]) for i, row in sector_df.sort_values(by=['industry', 'ticker']).iterrows()]
                table_body = [html.Tbody(table_rows)]
                sector_table = dbc.Table(table_header + table_body, bordered=False, hover=True, striped=True, className="small")
                sector_tables.extend([sector_header, sector_table])
            body = html.Div([summary] + sector_tables)
        return True, title, body
    if triggered_id == 'breakout-modal-close-btn':
        return False, "", ""
    return is_open, "", ""

# --- REFACTORED HISTORICAL CHARTS CALLBACK ---
@callback(
    Output('historical-breadth-chart', 'figure'),
    Output('ad-line-chart', 'figure'),
    Output('trend-composition-chart', 'figure'),
    Output('date-picker-range', 'start_date'),
    Output('date-picker-range', 'end_date'),
    [Output(f"btn-{text.lower()}", 'active') for text in ["1M", "3M", "6M", "1Y", "2Y"]],
    [Input(f"btn-{text.lower()}", 'n_clicks') for text in ["1M", "3M", "6M", "1Y", "2Y"]],
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
    Input(ThemeSwitchAIO.ids.switch("theme"), "value")
)
def update_historical_charts(*args):
    toggle_on = args[-1]
    template = "plotly_dark" if toggle_on else "plotly_white"
    start_date_str, end_date_str = args[-3], args[-2]
    button_id = str(ctx.triggered_id) if ctx.triggered_id else 'btn-1y'
    active_buttons = [button_id == f"btn-{t.lower()}" for t in ["1M", "3M", "6M", "1Y", "2Y"]]

    empty_fig = go.Figure().update_layout(title_text="No Data Available", template=template, xaxis_visible=False, yaxis_visible=False)
    
    if not engine:
        return empty_fig, empty_fig, empty_fig, None, None, *active_buttons

    max_date_query = "SELECT MAX(date) FROM daily_stock_analytics"
    try:
        max_date = pd.read_sql(max_date_query, engine).iloc[0, 0]
        if max_date is None or pd.isna(max_date):
            return empty_fig, empty_fig, empty_fig, None, None, *active_buttons
    except Exception:
        return empty_fig, empty_fig, empty_fig, None, None, *active_buttons

    if not start_date_str or not end_date_str:
        end_date_dt = pd.to_datetime(max_date) # type: ignore
        start_date_dt = end_date_dt - timedelta(days=365)
    else:
        end_date_dt = pd.to_datetime(end_date_str)
        start_date_dt = pd.to_datetime(start_date_str)

    if button_id and 'btn' in button_id:
        days = {'btn-1m': 30, 'btn-3m': 90, 'btn-6m': 182, 'btn-1y': 365, 'btn-2y': 730}
        end_date_dt = pd.to_datetime(max_date) # type: ignore
        start_date_dt = end_date_dt - timedelta(days=days.get(button_id, 365))

    start_date_sql = start_date_dt.strftime('%Y-%m-%d')
    end_date_sql = end_date_dt.strftime('%Y-%m-%d')

    # --- THIS IS THE FIX: Specify d.date or a.date ---
    breadth_query = f"""
    WITH daily_stats AS (
        SELECT
            d.date, -- Changed from 'date' to 'd.date'
            COUNT(*) AS total,
            SUM(CASE WHEN d.adj_close > a.ma_200 THEN 1 ELSE 0 END) AS above_200,
            SUM(CASE WHEN d.adj_close > a.ma_50 THEN 1 ELSE 0 END) AS above_50,
            SUM(CASE WHEN d.adj_close > a.ma_20 THEN 1 ELSE 0 END) AS above_20
        FROM daily_stock_data d
        JOIN daily_stock_analytics a ON d.ticker = a.ticker AND d.date = a.date
        WHERE d.date BETWEEN '{start_date_sql}' AND '{end_date_sql}'
        GROUP BY d.date -- Changed from 'date' to 'd.date'
    )
    SELECT
        date,
        100.0 * above_20 / total AS pct_above_20,
        100.0 * above_50 / total AS pct_above_50,
        100.0 * above_200 / total AS pct_above_200
    FROM daily_stats
    ORDER BY date;
    """
    # --- END OF FIX ---
    
    filtered_breadth_df = pd.read_sql_query(breadth_query, engine, index_col='date', parse_dates=['date'])
    
    breadth_fig = go.Figure(layout=dict(template=template, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)))
    breadth_fig.add_traces([go.Scatter(x=filtered_breadth_df.index, y=filtered_breadth_df[c], name=n) for c, n in [('pct_above_20', '% > 20D MA'), ('pct_above_50', '% > 50D MA'), ('pct_above_200', '% > 200D MA')]])
    breadth_fig.update_layout(title="Historical Market Breadth", yaxis_range=[0, 100])

    filtered_indicators_df = market_indicators_df.loc[start_date_dt:end_date_dt]
    filtered_spx_df = spx_df.loc[start_date_dt:end_date_dt]
    ad_fig = make_subplots(specs=[[{"secondary_y": True}]])
    ad_fig.add_trace(go.Scatter(x=filtered_spx_df.index, y=filtered_spx_df['hlcc4'], name="S&P 500 Price"), secondary_y=False)
    ad_fig.add_trace(go.Scatter(x=filtered_indicators_df.index, y=filtered_indicators_df['ad_line'], name="A/D Line"), secondary_y=True)
    ad_fig.update_layout(template=template, title_text="S&P 500 vs. Advance/Decline Line", legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))

    trend_query = f"""
    SELECT date, trend, COUNT(ticker) as count FROM daily_stock_analytics
    WHERE date BETWEEN '{start_date_sql}' AND '{end_date_sql}' GROUP BY date, trend ORDER BY date;
    """
    trend_df = pd.read_sql_query(trend_query, engine, parse_dates=['date'])
    trend_pivot = trend_df.pivot(index='date', columns='trend', values='count').fillna(0)
    filtered_trend_df = (100 * trend_pivot.div(trend_pivot.sum(axis=1), axis=0))

    trend_fig = go.Figure(layout=dict(template=template, yaxis=dict(range=[0, 100], ticksuffix='%'), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)))
    trend_fig.add_traces([go.Scatter(x=filtered_trend_df.index, y=filtered_trend_df.get(c, pd.Series(dtype='float64')), name=n, stackgroup='one', line=dict(color=co), fillcolor=fco) for c, n, co, fco in [('Uptrend', 'Uptrend', 'rgba(39, 174, 96, 0.8)', 'rgba(39, 174, 96, 0.5)'), ('Sideways', 'Sideways', 'rgba(243, 156, 18, 0.8)', 'rgba(243, 156, 18, 0.5)'), ('Downtrend', 'Downtrend', 'rgba(231, 76, 60, 0.8)', 'rgba(231, 76, 60, 0.5)')]])
    trend_fig.update_layout(title_text="Market Trend Composition")

    return breadth_fig, ad_fig, trend_fig, start_date_dt.date(), end_date_dt.date(), *active_buttons

# --- Breakout Chart Callback (No changes) ---
@callback(
    Output('breakout-chart', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
    Input(ThemeSwitchAIO.ids.switch("theme"), "value")
)
def update_breakout_chart(start_date, end_date, toggle_on):
    template = "plotly_dark" if toggle_on else "plotly_white"
    if market_indicators_df.empty or not start_date or not end_date:
        return go.Figure().update_layout(title_text="Not Enough Data", template=template)
    filtered_df = market_indicators_df.loc[start_date:end_date]
    fig = make_subplots(specs=[[{"secondary_y": True}]])
    fig.add_trace(go.Bar(x=filtered_df.index, y=filtered_df['high_volume_breakout_count'], name='Breakout Count'), secondary_y=False)
    fig.add_trace(go.Scatter(x=filtered_df.index, y=filtered_df['pct_above_avg_volume'], name='% Above Avg. Volume'), secondary_y=True)
    fig.update_layout(template=template, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
    fig.update_yaxes(title_text="Breakout Count", secondary_y=False)
    fig.update_yaxes(title_text="% Stocks > Avg. Volume", secondary_y=True)
    return fig

# --- Macro Chart Callback (No changes) ---
@callback(
    Output('macro-chart', 'figure'),
    Input('date-picker-range', 'start_date'),
    Input('date-picker-range', 'end_date'),
    Input(ThemeSwitchAIO.ids.switch("theme"), "value")
)
def update_macro_chart(start_date, end_date, toggle_on):
    template = "plotly_dark" if toggle_on else "plotly_white"
    if market_indicators_df.empty or spx_df.empty or not start_date or not end_date:
        return go.Figure().update_layout(title_text="No Macro Data Loaded", template=template)
    filtered_df = market_indicators_df.loc[start_date:end_date]
    filtered_spx = spx_df.loc[start_date:end_date]
    fig = make_subplots(rows=2, cols=1, shared_xaxes=True, vertical_spacing=0.1, row_heights=[0.7, 0.3], specs=[[{"secondary_y": True}], [{}]])
    fig.add_trace(go.Scatter(x=filtered_spx.index, y=filtered_spx['hlcc4'], name="S&P 500 Price"), row=1, col=1, secondary_y=False)
    fig.add_trace(go.Scatter(x=filtered_df.index, y=filtered_df['dff'], name="Fed Funds Rate (%)"), row=1, col=1, secondary_y=True)
    bar_data = filtered_df['t10y2y'].dropna()
    if not bar_data.empty:
        colors = ['#d62728' if x < 0 else '#2ca02c' for x in bar_data]
        fig.add_trace(go.Bar(x=bar_data.index, y=bar_data, name="10Y-2Y Spread", marker_color=colors), row=2, col=1)
    fig.add_hline(y=0, line_width=1, line_dash="dash", line_color="grey", row=2, col=1) # type: ignore
    fig.update_layout(template=template, legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1), showlegend=True)
    fig.update_yaxes(title_text="S&P 500 Price", row=1, col=1, secondary_y=False)
    fig.update_yaxes(title_text="Fed Funds Rate (%)", row=1, col=1, secondary_y=True, showgrid=False)
    fig.update_yaxes(title_text="10Y-2Y Spread", row=2, col=1)
    fig.update_xaxes(title_text="Date", row=2, col=1)
    fig.update_xaxes(showticklabels=False, row=1, col=1)
    return fig

# --- REFACTORED AI SUMMARY CALLBACK ---
@callback(
    Output("ai-summary-output-container", "children"),
    Input("generate-ai-summary-btn", "n_clicks"),
    prevent_initial_call=True
)
def update_ai_summary(n_clicks):
    if n_clicks is None or n_clicks < 1 or not engine:
        raise PreventUpdate

    # --- THIS IS THE FIX: Specify d.date to resolve ambiguity ---
    breadth_query = """
    WITH date_calcs AS (
        SELECT
            d.date, -- Changed from 'date'
            100.0 * SUM(CASE WHEN d.adj_close > a.ma_50 THEN 1 ELSE 0 END) / COUNT(*) as pct_above_50,
            100.0 * SUM(CASE WHEN d.adj_close > a.ma_200 THEN 1 ELSE 0 END) / COUNT(*) as pct_above_200
        FROM daily_stock_data d
        JOIN daily_stock_analytics a ON d.ticker = a.ticker AND d.date = a.date
        GROUP BY d.date -- Changed from 'date'
        ORDER BY d.date DESC
        LIMIT 6
    )
    SELECT * FROM date_calcs ORDER BY date ASC;
    """
    # --- END OF FIX ---
    
    try:
        breadth_for_ai = pd.read_sql_query(breadth_query, engine, index_col='date', parse_dates=['date'])
        
        if len(breadth_for_ai) < 6:
            return html.P("Not enough historical data to generate trend analysis.", className="text-warning")

        # The indicators are already loaded globally and are small, so we can reuse market_indicators_df
        summary_text = generate_market_summary(breadth_for_ai, market_indicators_df)
    except Exception as e:
        logging.error(f"AI summary callback failed during data query: {e}")
        return html.P("Failed to retrieve data for AI summary.", className="text-danger")

    return dcc.Markdown(summary_text, className="mt-2", style={"white-space": "pre-wrap"})

@callback(
    Output("latest-date-display", "children"),
    Input("gauge-ma20", "figure") # Trigger this after the gauges are loaded
)
def update_latest_date_display(_):
    if pd.notna(latest_date):
        return f"Data as of: {pd.to_datetime(latest_date).strftime('%Y-%m-%d')}"
    return "Data as of: N/A"