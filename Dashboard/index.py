from dash import dcc, html, Input, Output, callback
import dash_bootstrap_components as dbc
from dash_bootstrap_templates import ThemeSwitchAIO
from Dashboard.app import app, url_theme_light, url_theme_dark
from Dashboard.layouts import macro, meso
from Dashboard.callbacks import macro_callbacks, meso_callbacks


# --- The Master Application Layout ---
app.layout = dbc.Container(
    [
        dbc.Row(
            [
                dbc.Col(html.H1("Market Health Dashboard")),
                dbc.Col(ThemeSwitchAIO(aio_id="theme", themes=[url_theme_light, url_theme_dark]), width="auto"),
            ],
            align="center",
            className="my-4",
        ),
        dbc.Tabs(
            [
                dbc.Tab(label="Macro Analysis", tab_id="macro"),
                dbc.Tab(label="Sector & Industry Analysis", tab_id="meso"),
            ],
            id="tabs",
            active_tab="macro",
            persistence=True,
        ),
        html.Div(id="tab-content", className="mt-4"),
    ],
    fluid=True,
    className="dbc"
)

# --- Callbacks for the Master Layout ---
@callback(Output("tab-content", "children"), Input("tabs", "active_tab"))
def render_tab_content(active_tab):
    if active_tab == "meso":
        return meso.layout
    return macro.layout

# --- Run the Application ---
if __name__ == "__main__":
    # Use the correct `app.run` for modern Dash versions
    app.run(debug=False)