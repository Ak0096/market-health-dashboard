# In Dashboard/layouts/macro.py

from dash import dcc, html
import dash_bootstrap_components as dbc

# --- The Definitive, Corrected Macro Layout ---
layout = dbc.Container([
    dbc.Row([
        dbc.Col(html.H5(id="latest-date-display", className="text-center text-muted mb-4"), width=12)
    ]),
    
    # AI Analysis Section
    dbc.Row([
        dbc.Col(
            dbc.Card([
                dbc.CardHeader(html.H5("AI Market Health Analysis")),
                dbc.CardBody([
                    dbc.Button("Generate Summary", id="generate-ai-summary-btn", color="primary", className="mb-3"),
                    dcc.Loading(
                        id="loading-ai-summary",
                        children=[html.Div(id="ai-summary-output-container")],
                        type="default"
                    )
                ])
            ]),
            width=12
        )
    ], className="mb-4"),

    html.Hr(),

    # Gauges Section
    dbc.Row([
        dbc.Col(dbc.Card([dbc.CardHeader(html.H5("% Above 20-Day MA", className="card-title text-center")), dbc.CardBody(dcc.Graph(id='gauge-ma20'))]), width=12, md=4),
        dbc.Col(dbc.Card([dbc.CardHeader(html.H5("% Above 50-Day MA", className="card-title text-center")), dbc.CardBody(dcc.Graph(id='gauge-ma50'))]), width=12, md=4),
        dbc.Col(dbc.Card([dbc.CardHeader(html.H5("% Above 200-Day MA", className="card-title text-center")), dbc.CardBody(dcc.Graph(id='gauge-ma200'))]), width=12, md=4),
    ], className="mb-4"),

    html.Hr(),

    # Chart Controls and Historical Breadth Chart
    dbc.Row([
        dbc.Col([
            dbc.Card([
                dbc.CardBody([
                    html.H4("Chart Controls", className="card-title"),
                    html.Hr(),
                    dbc.ButtonGroup(
                        [dbc.Button(text, id=f"btn-{text.lower()}", n_clicks=0, color="primary", outline=True) for text in ["1M", "3M", "6M", "1Y", "2Y"]],
                        className="d-grid gap-2 mb-3",
                    ),
                    dcc.DatePickerRange(
                        id='date-picker-range',
                        min_date_allowed=None,
                        max_date_allowed=None,
                        initial_visible_month=None,
                        start_date=None,
                        end_date=None,
                        className="mb-3"
                    ),
                ])
            ])
        ], width=12, md=3),
        dbc.Col([
            # --- FIX: Changed dbc.Body to dbc.CardBody ---
            dbc.Card([dbc.CardBody([dcc.Loading(dcc.Graph(id='historical-breadth-chart', style={'height': '60vh'}))])])
        ], width=12, md=9),
    ], className="mb-4"),

    # Trend Composition and A/D Line Charts
    dbc.Row([
        # --- FIX: Changed dbc.Body to dbc.CardBody ---
        dbc.Col(dbc.Card([dbc.CardBody([dcc.Loading(dcc.Graph(id='trend-composition-chart', style={'height': '60vh'}))])]), width=12, md=6, className="mb-4"),
        dbc.Col(dbc.Card([dbc.CardBody([dcc.Loading(dcc.Graph(id='ad-line-chart', style={'height': '60vh'}))])]), width=12, md=6, className="mb-4"),
    ]),

    html.Hr(),
    
    # Breakout Chart
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader(html.H5("High-Volume Breakouts")),
            dbc.CardBody([dcc.Loading(dcc.Graph(id='breakout-chart', style={'height': '60vh'}))])
        ]), width=12)
    ], className="mb-4"),

    # Macro Indicators Chart
    html.Hr(),
    dbc.Row([
        dbc.Col(dbc.Card([
            dbc.CardHeader(html.H5("Macro Indicators")),
            dbc.CardBody([dcc.Loading(dcc.Graph(id='macro-chart', style={'height': '60vh'}))])
        ]), width=12)
    ], className="mb-4"),
    
    # Breakout Modal
    dbc.Modal([
        dbc.ModalHeader(dbc.ModalTitle(id="breakout-modal-title")),
        dbc.ModalBody(id="breakout-modal-body"),
        dbc.ModalFooter(
            dbc.Button("Close", id="breakout-modal-close-btn", className="ms-auto", n_clicks=0)
        )
    ], id="breakout-modal", is_open=False, size="lg"),

], fluid=True)