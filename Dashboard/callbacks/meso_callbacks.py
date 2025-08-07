from dash import Input, Output, callback, dcc, html, ALL, ctx, State
from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
from dash_bootstrap_templates import ThemeSwitchAIO
from Dashboard.data import latest_stock_analytics_df
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import pandas as pd

from Dashboard.app import app
from Dashboard.data import group_analytics_df, industry_to_sector_map # Import the new map

def create_stat_card(label, value, status_class=""):
    return dbc.Col(dbc.Card([dbc.CardHeader(label, className="text-center small"), dbc.CardBody(html.H4(value, className=f"text-center {status_class}"))]), className="mb-2")

def get_status_class(value, threshold_bull, threshold_bear):
    if pd.isna(value): return ""
    if value >= threshold_bull: return "text-success"
    if value <= threshold_bear: return "text-danger"
    return "text-warning"

@callback(Output('group-select-dropdown', 'options'), Input('analysis-mode-toggle', 'value'))
def update_dropdown_options(analysis_mode):
    if group_analytics_df.empty: return []
    return sorted(group_analytics_df[group_analytics_df['group_type'] == analysis_mode]['group_name'].unique())

@callback(
    Output('meso-date-picker-range', 'min_date_allowed'), Output('meso-date-picker-range', 'max_date_allowed'),
    Output('meso-date-picker-range', 'initial_visible_month'), Output('meso-date-picker-range', 'start_date'),
    Output('meso-date-picker-range', 'end_date'),
    Input('analysis-mode-toggle', 'value')
)
def set_initial_date_picker_state(analysis_mode):
    if group_analytics_df.empty: return None, None, None, None, None
    min_date, max_date = group_analytics_df['analysis_date'].min().date(), group_analytics_df['analysis_date'].max().date()
    start_date = (pd.to_datetime(max_date) - pd.DateOffset(years=1)).date()
    return min_date, max_date, max_date, start_date, max_date

@callback(
    Output('meso-main-title', 'children'), Output('meso-main-subtitle', 'children'),
    Output('group-select-label', 'children'), Output('heatmap-container', 'style'),
    Output('group-analysis-container', 'style'), Output('back-button-container', 'style'),
    Output('stats-container', 'children'), Output('summary-title', 'children'),
    Output('summary-content', 'children'), Output('heatmap-container', 'children'),
    Output('rs-chart', 'figure'), Output('momentum-chart', 'figure'),
    Input('analysis-mode-toggle', 'value'), Input('group-select-dropdown', 'value'),
    Input('meso-date-picker-range', 'start_date'), Input('meso-date-picker-range', 'end_date'),
    Input(ThemeSwitchAIO.ids.switch("theme"), "value")
)
def update_meso_view(analysis_mode, selected_group, start_date, end_date, toggle_on):
    template = "plotly_dark" if toggle_on else "plotly_white"
    if group_analytics_df.empty:
        return "Sector & Industry Analysis", "No data loaded.", "Select Group", {}, {}, {'display': 'none'}, [], "Error", "Data could not be loaded.", [], go.Figure(layout={'template': template}), go.Figure(layout={'template': template})

    try:
        mask = (group_analytics_df['analysis_date'] >= pd.to_datetime(start_date)) & (group_analytics_df['analysis_date'] <= pd.to_datetime(end_date))
        filtered_df = group_analytics_df.loc[mask]
        if filtered_df.empty: raise ValueError("No data in selected date range")
    except (TypeError, ValueError):
        return "Sector & Industry Analysis", "Please select a valid date range.", "Select Group", {}, {}, {'display': 'none'}, [], "Error", "Waiting for valid date range...", [], go.Figure(layout={'template': template}), go.Figure(layout={'template': template})

    mode_cap = analysis_mode.title()
    title, subtitle, select_label = f"Professional {mode_cap} RS Dashboard", f"Comprehensive {analysis_mode} relative strength analysis vs. S&P 500 benchmark", f"Select {mode_cap}"

    if not selected_group:
        # --- OVERVIEW MODE ---
        style_heatmap, style_analysis, style_back_btn = {'display': 'block'}, {'display': 'none'}, {'display': 'none'}
        latest_date = filtered_df['analysis_date'].max()
        latest_df = filtered_df[(filtered_df['analysis_date'] == latest_date) & (filtered_df['group_type'] == analysis_mode)]
        if latest_df.empty: return title, subtitle, select_label, style_heatmap, style_analysis, style_back_btn, [], f"{mode_cap} Overview", "No data for this time period.", [], go.Figure(layout={'template': template}), go.Figure(layout={'template': template})

        total = len(latest_df)
        pct_200 = round(100*latest_df['above_rs_200sma'].sum()/total)
        stats = dbc.Row([create_stat_card(f"Total {mode_cap}s", total), create_stat_card("% > 200 SMA", f"{pct_200}%", get_status_class(pct_200,70,30)), create_stat_card("% > 50 SMA", f"{round(100*latest_df['above_rs_50sma'].sum()/total)}%", get_status_class(round(100*latest_df['above_rs_50sma'].sum()/total),70,30)), create_stat_card("% > 20 SMA", f"{round(100*latest_df['above_rs_20sma'].sum()/total)}%", get_status_class(round(100*latest_df['above_rs_20sma'].sum()/total),70,30))])
        
        summary_title = f"{mode_cap} Overview Summary"
        sorted_df = latest_df.sort_values(by='group_rs_value', ascending=False)
        leader, laggard = sorted_df.iloc[0]['group_name'], sorted_df.iloc[-1]['group_name']
        summary = f"Breadth: {latest_df['above_rs_200sma'].sum()}/{total} {analysis_mode}s ({pct_200}%) are in long-term uptrends.\n\nLeadership: {leader} is strongest, {laggard} is weakest."
        
        heatmap_data = latest_df.sort_values(by=['above_rs_200sma','above_rs_50sma','group_rs_roc_20','group_rs_value'], ascending=[False,False,False,False])
        
        # --- START: MODIFIED TABLE CREATION LOGIC ---
        table_rows = []
        if analysis_mode == 'industry':
            table_header = [html.Thead(html.Tr([html.Th("Industry"), html.Th("Sector"), html.Th("RS Val"), html.Th("20D Mom"), html.Th("vs 50D"), html.Th("vs 200D")]))]
            for i, row in heatmap_data.iterrows():
                roc_val = f"{row['group_rs_roc_20']:.1f}%" if pd.notna(row['group_rs_roc_20']) else "N/A"
                sector = industry_to_sector_map.get(row['group_name'], 'N/A')
                cells = [
                    html.Td(row['group_name']),
                    html.Td(sector),
                    html.Td(f"{row['group_rs_value']:.3f}"),
                    html.Td(roc_val, className=get_status_class(row.get('group_rs_roc_20', 0), 5, -5)),
                    html.Td("Above" if row['above_rs_50sma'] else "Below", className="text-success" if row['above_rs_50sma'] else "text-danger"),
                    html.Td("Above" if row['above_rs_200sma'] else "Below", className="text-success" if row['above_rs_200sma'] else "text-danger"),
                ]
                table_rows.append(html.Tr(cells, id={'type':'heatmap-row','index':row['group_name']}, className='clickable-row', n_clicks=0))
        else: # Sector mode
            table_header = [html.Thead(html.Tr([html.Th("Sector"), html.Th("RS Val"), html.Th("20D Mom"), html.Th("vs 50D"), html.Th("vs 200D")]))]
            for i, row in heatmap_data.iterrows():
                roc_val = f"{row['group_rs_roc_20']:.1f}%" if pd.notna(row['group_rs_roc_20']) else "N/A"
                cells = [
                    html.Td(row['group_name']),
                    html.Td(f"{row['group_rs_value']:.3f}"),
                    html.Td(roc_val, className=get_status_class(row.get('group_rs_roc_20', 0), 5, -5)),
                    html.Td("Above" if row['above_rs_50sma'] else "Below", className="text-success" if row['above_rs_50sma'] else "text-danger"),
                    html.Td("Above" if row['above_rs_200sma'] else "Below", className="text-success" if row['above_rs_200sma'] else "text-danger"),
                ]
                table_rows.append(html.Tr(cells, id={'type':'heatmap-row','index':row['group_name']}, className='clickable-row', n_clicks=0))

        table = dbc.Table(table_header + [html.Tbody(table_rows)], bordered=False, hover=True, striped=True, responsive=True, className="small")
        heatmap = [html.H4(f"{mode_cap} Performance Ranking", className="card-title"), html.Hr(), table]
        # --- END: MODIFIED TABLE CREATION LOGIC ---

        return title, subtitle, select_label, style_heatmap, style_analysis, style_back_btn, stats, summary_title, summary, heatmap, go.Figure(layout={'template': template}), go.Figure(layout={'template': template})
    else:
        # --- GROUP DETAIL MODE ---
        style_heatmap, style_analysis, style_back_btn = {'display': 'none'}, {'display': 'block'}, {'display': 'block'}
        group_df = filtered_df[(filtered_df['group_type'] == analysis_mode) & (filtered_df['group_name'] == selected_group)].sort_values('analysis_date')
        if group_df.empty: return title, subtitle, select_label, style_heatmap, style_analysis, style_back_btn, html.Div("No data for this group."), "Summary", "", [], go.Figure(layout={'template': template}), go.Figure(layout={'template': template})
        
        latest = group_df.iloc[-1]
        stats = dbc.Row([create_stat_card("Current RS", f"{latest['group_rs_value']:.3f}","text-success" if latest['above_rs_200sma'] else "text-danger"), create_stat_card("200D SMA", f"{latest['group_rs_sma_200']:.3f}"), create_stat_card("50D SMA", f"{latest['group_rs_sma_50']:.3f}"), create_stat_card("20D SMA", f"{latest['group_rs_sma_20']:.3f}")])
        summary_title, trend = f"Analysis: {selected_group}", ("uptrend" if latest['above_rs_200sma'] else "downtrend")
        summary = f"The {selected_group} {analysis_mode} is in a long-term {trend}, with its RS value { 'above' if latest['above_rs_200sma'] else 'below'} its 200-day MA."
        
        rs_fig = go.Figure(layout={'template':template,'height':350}).add_traces([go.Scatter(x=group_df['analysis_date'], y=group_df[c], name=n, line=dict(color=co, dash=d)) for c,n,co,d in [('group_rs_value','Group RS','#1f77b4','solid'),('group_rs_sma_200','200D SMA','#d62728','dot'),('group_rs_sma_50','50D SMA','#2ca02c','dot')]]).update_layout(title="<b>RS Trend</b>", margin=dict(t=30,b=10), legend=dict(orientation="h",yanchor="bottom",y=1.02,xanchor="right",x=1))
        momentum_fig = go.Figure(layout={'template':template,'height':350}).add_trace(go.Bar(x=group_df['analysis_date'],y=group_df['group_rs_roc_20'],name='20D ROC',marker_color=["#2ca02c" if x>=0 else "#d62728" for x in group_df['group_rs_roc_20']])).update_layout(title="<b>RS Momentum (ROC)</b>", margin=dict(t=30,b=10), yaxis_ticksuffix='%')

        return title, subtitle, select_label, style_heatmap, style_analysis, style_back_btn, stats, summary_title, summary, [], rs_fig, momentum_fig

@callback(
    Output('group-select-dropdown', 'value'),
    Input({'type': 'heatmap-row', 'index': ALL}, 'n_clicks'),
    State('group-select-dropdown', 'value'),
    prevent_initial_call=True
)
def update_dropdown_from_table_click(n_clicks, current_value):
    # This check prevents a crash if no clicks have occurred.
    if not n_clicks or all(c == 0 for c in n_clicks):
        raise PreventUpdate

    # THIS IS THE FIX: Add a guard clause for ctx.triggered_id
    triggered_id = ctx.triggered_id
    if not triggered_id:
        raise PreventUpdate

    clicked_group = triggered_id['index']
    if clicked_group == current_value:
        raise PreventUpdate
    return clicked_group

@callback(Output('group-select-dropdown', 'value', allow_duplicate=True), Input('back-to-overview-btn', 'n_clicks'), prevent_initial_call=True)
def go_back_to_overview(n_clicks):
    return None

@callback(
    Output('meso-date-picker-range', 'start_date', allow_duplicate=True),
    Output('meso-date-picker-range', 'end_date', allow_duplicate=True),
    [Input(f"meso-btn-{text.lower()}",'n_clicks') for text in ["3M","6M","1Y","2Y","5Y"]],
    prevent_initial_call=True
)
def update_meso_date_range_from_buttons(*args):
    button_id = ctx.triggered_id if ctx.triggered_id else 'meso-btn-1y'
    if group_analytics_df.empty: raise PreventUpdate
    end_date = group_analytics_df['analysis_date'].max()
    deltas = {'3m':3,'6m':6,'1y':12,'2y':24,'5y':60}
    offset = pd.DateOffset(months=deltas.get(button_id.split('-')[-1], 12))
    return (end_date - offset).date(), end_date.date()

@callback(
    Output("rs-leaders-table-container", "children"),
    Input('meso-main-title', 'children') 
)
def update_rs_leaders_table(main_title):
    if latest_stock_analytics_df.empty:
        return html.P("Stock analytics data not loaded.", className="text-danger")

    # --- FIX: Exclude the benchmark ticker from the list ---
    leaders_df = latest_stock_analytics_df[
        (latest_stock_analytics_df['trend'] == 'Uptrend') &
        (latest_stock_analytics_df['ticker'] != '^GSPC') # <-- ADD THIS CONDITION
    ].sort_values(by='rs', ascending=False).head(25)

    if leaders_df.empty:
        return html.P("No stocks currently meet the criteria for leadership (Uptrend with high RS).", className="text-warning")

    # The rest of the function remains the same...
    table_header = [
        html.Thead(html.Tr([
            html.Th("Ticker"),
            html.Th("Sector"),
            html.Th("Industry"),
            html.Th("RS Rating"),
            html.Th("YTD Perf %")
        ]))
    ]

    table_rows = []
    for i, row in leaders_df.iterrows():
        table_rows.append(html.Tr([
            html.Td(row['ticker']),
            html.Td(row['sector']),
            html.Td(row['industry']),
            html.Td(f"{row['rs']:.3f}"),
            html.Td(f"{row['perf_ytd']:.2f}%", className="text-success" if row['perf_ytd'] > 0 else "text-danger")
        ]))
    
    table_body = [html.Tbody(table_rows)]
    
    return dbc.Table(table_header + table_body, bordered=False, hover=True, striped=True, responsive=True)