import redis
import time
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()  # Loads variables from .env

def clear_screen():
    if os.name == 'nt':
        os.system('cls')
    else:
        os.system('clear')

def press_enter_to_continue():
    input("\nPress Enter to continue...\n")

def format_timestamp(ts_str):
    try:
        ts_float = float(ts_str)
        local_time = datetime.fromtimestamp(ts_float)
        return local_time.strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        return ts_str

# --- Redis connection details ---
username = os.getenv('REDIS_USERNAME')
password = os.getenv('REDIS_PASSWORD')
host = os.getenv('REDIS_HOST')
port = os.getenv('REDIS_PORT')
stream_key = 'user_activity_log'
group = 'activity_consumers'
consumer = 'consumer1'

redis_url = f'redis://{username}:{password}@{host}:{port}/0'
r = redis.from_url(redis_url)

# Create consumer group if it doesn't exist
try:
    r.xgroup_create(stream_key, group, id='0', mkstream=True)
except redis.exceptions.ResponseError as e:
    # Ignore error if group already exists
    if "BUSYGROUP" not in str(e):
        raise

clear_screen()
print("Welcome to the Redis Stream CONSUMER demo!")
print("-" * 60)
print(f"This script reads user activity events from the Redis stream")
print(f"'{stream_key}', using consumer group '{group}'.")
print("It will display the events in a clean table and remember progress.")
press_enter_to_continue()
clear_screen()

def print_header():
    print(f"{'ID':<20} {'User':<10} {'Action':<15} {'Timestamp':<20}")
    print("-" * 65)

print("Starting to consume... Press Ctrl+C to stop.\n")
print_header()

try:
    while True:
        resp = r.xreadgroup(group, consumer, {stream_key: '>'}, count=5, block=2000)
        if resp:
            for stream, messages in resp:
                for msg_id, msg_data in messages:
                    # Decode msg_id if bytes
                    msg_id_str = msg_id.decode() if isinstance(msg_id, bytes) else str(msg_id)
                    user = msg_data.get(b'user', b'').decode().capitalize()
                    action = msg_data.get(b'action', b'').decode().replace('_', ' ').capitalize()
                    ts = format_timestamp(msg_data.get(b'timestamp', b'0').decode())

                    print(f"{msg_id_str:<20} {user:<10} {action:<15} {ts:<20}")

                    # Acknowledge message to mark processed
                    r.xack(stream_key, group, msg_id)
        else:
            print("No new messages. Waiting...")
            time.sleep(2)

except KeyboardInterrupt:
    print("\nConsumer stopped by user.")
