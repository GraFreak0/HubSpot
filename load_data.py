import os 
import clickhouse_connect
from dotenv import load_dotenv

load_dotenv()
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", 8123))
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

if __name__ == '__main__':
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        # port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        secure=True,  # Set to True if using HTTPS
    )
    print("Result:", client.query("SELECT 1").result_set[0][0])