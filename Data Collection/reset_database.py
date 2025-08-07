import json
from sqlalchemy import create_engine, text

# This script will connect to your database and drop the 'daily_stock_data' table
# to ensure it can be created correctly by the main pipeline.

print("--- Database Reset Script ---")

# 1. Load the configuration to get database details
print("Loading configuration...")
try:
    with open('data collection/config.json', 'r') as f:
        config = json.load(f)['database']
    print("✅ Config loaded.")
except Exception as e:
    print(f"❌ Could not load config.json: {e}")
    exit()

# 2. Connect to the database
try:
    engine_url = f"postgresql+psycopg2://{config['user']}:{config['password']}@{config['host']}:{config['port']}/{config['dbname']}"
    engine = create_engine(engine_url)
    connection = engine.connect()
    print("✅ Database connection successful.")
except Exception as e:
    print(f"❌ Could not connect to database: {e}")
    exit()

# 3. Drop the table
try:
    print("Attempting to drop 'daily_stock_data' table...")
    # We use a transaction to ensure this is handled safely
    with connection.begin() as trans:
        connection.execute(text("DROP TABLE IF EXISTS daily_stock_data;"))
    print("✅ Table 'daily_stock_data' dropped successfully.")
except Exception as e:
    print(f"❌ Error dropping table: {e}")
finally:
    connection.close()
    print("Connection closed.")

print("\n--- Reset Complete ---")
print("You can now run your main data_pipeline.py script.")