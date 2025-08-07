import dash
import dash_bootstrap_components as dbc

# Define better theme combinations for true dark/light contrast
# DARKLY - A proper dark theme with dark backgrounds
# LUX - A clean, modern light theme with good contrast
url_theme_light = dbc.themes.MINTY
url_theme_dark = dbc.themes.DARKLY

# Alternative good combinations:
# Light: FLATLY, LUX, MINTY, COSMO
# Dark: DARKLY, CYBORG, SLATE, SOLAR

# The ThemeSwitchAIO component will handle switching between themes.
# We start with the light theme.
app = dash.Dash(
    __name__,
    external_stylesheets=[url_theme_light, dbc.icons.FONT_AWESOME],
    suppress_callback_exceptions=True
)
app.title = "Market Health Dashboard"