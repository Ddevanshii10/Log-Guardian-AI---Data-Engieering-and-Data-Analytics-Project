import os
from dotenv import load_dotenv
from databricks import sql

load_dotenv()

server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
http_path = os.getenv("DATABRICKS_HTTP_PATH")
access_token = os.getenv("DATABRICKS_TOKEN")
catalog = os.getenv("DATABRICKS_CATALOG", "log-analytics")

print("Testing Databricks connection...")
print(f"Server hostname found: {bool(server_hostname)}")
print(f"HTTP path found: {bool(http_path)}")
print(f"Token found: {bool(access_token)}")

if not all([server_hostname, http_path, access_token]):
    print("\nERROR: One or more Databricks environment variables are missing.")
    print("Check your .env file.")
    raise SystemExit(1)

try:
    with sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    ) as connection:

        with connection.cursor() as cursor:
            cursor.execute("SELECT 1 AS connection_test")
            result = cursor.fetchall()

            print("\nSUCCESS! Databricks connection established.")
            print("Test query result:", result)

            cursor.execute(f"DESCRIBE TABLE `{catalog}`.gold.vw_dash_service_overview")
            schema = cursor.fetchall()
            if not schema:
                raise RuntimeError("Gold view returned no schema")
            print("SUCCESS: Gold schema accessible")

            cursor.execute(f"SELECT * FROM `{catalog}`.gold.vw_dash_service_overview LIMIT 1")
            print("SUCCESS: Live dashboard data retrieved:", bool(cursor.fetchall()))

except Exception as e:
    print("\nCONNECTION FAILED")
    print("Error:", str(e))