\# US Market Health Dashboard



This project is an end-to-end data pipeline and web dashboard for analyzing the health of the US stock market. It automatically ingests market data, computes a wide range of technical and macroeconomic indicators, and presents them in an interactive dashboard.



The system includes an AI-powered analyst (using Google's Gemini API) to provide natural language summaries of market conditions.



\## Features



\- \*\*Automated Data Pipeline:\*\* Efficiently downloads and updates daily stock prices and corporate actions using an incremental approach.

\- \*\*Comprehensive Analytics:\*\* Calculates dozens of indicators, including market breadth, relative strength, trend analysis, and macroeconomic factors.

\- \*\*Scalable Architecture:\*\* The data pipeline and dashboard are designed to be memory-efficient and handle a growing database.

\- \*\*AI-Powered Insights:\*\* Integrates with the Gemini API to provide sophisticated, data-driven summaries of market health.

\- \*\*Interactive Dashboard:\*\* A web-based interface built with Plotly Dash for visualizing all indicators.



\## Setup



1\.  \*\*Clone the repository:\*\*

&nbsp;   ```bash

&nbsp;   git clone https://github.com/Ak0096/market-health-dashboard.git

&nbsp;   cd YOUR\_REPOSITORY\_NAME

&nbsp;   ```



2\.  \*\*Create a virtual environment and install dependencies:\*\*

&nbsp;   ```bash

&nbsp;   python -m venv venv

&nbsp;   venv\\Scripts\\activate

&nbsp;   pip install -r requirements.txt

&nbsp;   ```

&nbsp;   

3\.  \*\*Set up the database:\*\*

&nbsp;   - This project requires a PostgreSQL database.

&nbsp;   - Create a database (e.g., `us\_market.db`).

&nbsp;   

4\.  \*\*Configure the application:\*\*

&nbsp;   - Create a `config.json` file in the `Data Collection` directory. A template or example should be provided here.

&nbsp;   - Create a `.env` file in the project root for your secret API keys:

&nbsp;   ```

&nbsp;   GEMINI\_API\_KEY="your\_gemini\_key\_here"

&nbsp;   FRED\_API\_KEY="your\_fred\_key\_here"

&nbsp;   ```



\## Usage



1\.  \*\*Run the data pipeline:\*\*

&nbsp;   ```bash

&nbsp;   python "Data Collection/data\_pipeline.py"

&nbsp;   ```



2\.  \*\*Run the analytics computation:\*\*

&nbsp;   ```bash

&nbsp;   python "Data Collection/compute\_analytics.py"

&nbsp;   ```

&nbsp;   

3\.  \*\*Launch the dashboard:\*\*

&nbsp;   ```bash

&nbsp;   python Dashboard/index.py

&nbsp;   ```

