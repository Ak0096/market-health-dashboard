# In Dashboard/layouts/meso.py

from dash import dcc, html
import dash_bootstrap_components as dbc

# This layout is now static and includes persistence for a better UX.
layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H2(id='meso-main-title', className="text-primary"), width=12),
        dbc.Col(html.H5(id='meso-main-subtitle', className="text-muted"), width=12),
    ], className="text-center my-4"),
    html.Hr(),

    dbc.Row([
        dbc.Col([
            dbc.Label("Analysis Mode:", className="fw-bold"),
            dbc.RadioItems(
                options=[{'label': 'Sector Analysis', 'value': 'sector'}, {'label': 'Industry Analysis', 'value': 'industry'}],
                value='sector',
                id='analysis-mode-toggle',
                inline=True,
                className="mb-2",
                persistence=True,
                persistence_type='local'
            ),
        ], width=12, md=4),
        dbc.Col([
            dbc.Label(id='group-select-label', className="fw-bold"),
            dcc.Dropdown(
                id='group-select-dropdown',
                placeholder="Select a group or view overview...",
                clearable=True,
                persistence=True,
                persistence_type='local'
            ),
        ], width=12, md=8),
    ], className="mb-4"),

    dbc.Row([
        dbc.Col([
            dbc.Label("Select Timeframe:", className="fw-bold"),
            dbc.ButtonGroup(
                [dbc.Button(text, id=f"meso-btn-{text.lower()}", n_clicks=0, color="secondary", outline=True) for text in ["3M", "6M", "1Y", "2Y", "5Y"]],
            )
        ], width="auto"),
        dbc.Col([
            dbc.Label("Or Select Custom Date Range:", className="fw-bold"),
            dcc.DatePickerRange(
                id='meso-date-picker-range',
                min_date_allowed=None,
                max_date_allowed=None,
                initial_visible_month=None,
                start_date=None,
                end_date=None,
                className="w-100"
            ),
        ], width=True, className="ms-3"),
    ], className="mb-4 align-items-center"),

    dbc.Row([
        dbc.Col([
            html.Div(
                dbc.Button("← Back to Overview", id="back-to-overview-btn", color="secondary", className="w-100 mb-4"),
                id="back-button-container"
            ),
            dbc.Card(dbc.CardBody(id='stats-container'), className="mb-4"),
            dbc.Card(dbc.CardBody([
                html.H5(id='summary-title', className="card-title"),
                html.Hr(),
                html.P(id='summary-content', style={'whiteSpace': 'pre-wrap'})
            ])),
        ], width=12, lg=3),
        dbc.Col([
            dbc.Card(dbc.CardBody(id='heatmap-container'), className="mb-4"),
            html.Div(id='group-analysis-container', children=[
                dbc.Card(dbc.CardBody(dcc.Loading(dcc.Graph(id='rs-chart'))), className="mb-4"),
                dbc.Card(dbc.CardBody(dcc.Loading(dcc.Graph(id='momentum-chart')))),
            ]),
        ], width=12, lg=9),
    ]),

    # --- THIS IS THE SECTION YOU ARE MISSING ---
    html.Hr(className="my-4"),
    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardHeader(html.H5("Relative Strength Leaders (Top 25)")),
                dbc.CardBody(
                    dcc.Loading(
                        id="loading-rs-leaders",
                        children=[html.Div(id="rs-leaders-table-container")],
                        type="default"
                    )
                )
            ]),
            width=12
        )
    ], className="mb-4"),
    # --- END OF MISSING SECTION ---

], fluid=True)