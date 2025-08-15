import redis
import time
import random
import os
from dotenv import load_dotenv

load_dotenv()  # Loads variables from .env

def clear_screen():
    if os.name == 'nt':  # Windows
        os.system('cls')
    else:                # Linux, macOS, etc.
        os.system('clear')

def press_enter_to_continue():
    input("\nPress Enter to continue...\n")

username = os.getenv('REDIS_USERNAME')
password = os.getenv('REDIS_PASSWORD')
host = os.getenv('REDIS_HOST')
port = os.getenv('REDIS_PORT')
stream_key = 'user_activity_log'

redis_url = f'redis://{username}:{password}@{host}:{port}/0'
r = redis.from_url(redis_url)

users = ['alice', 'bob', 'carol']
actions = ['login', 'logout', 'purchase', 'update_profile']

clear_screen()
print("Welcome to the Redis Stream PRODUCER demo!")
print("-" * 55)
print("This script will continuously simulate user activity events")
print(f"and push them into the Redis stream: '{stream_key}'.")
print("You can monitor these events with a separate consumer process.")
press_enter_to_continue()
clear_screen()

print(f"Producing to stream '{stream_key}'... Press Ctrl+C to stop.\n")
try:
    while True:
        entry = {
            'user': random.choice(users),
            'action': random.choice(actions),
            'timestamp': str(time.time())
        }
        r.xadd(stream_key, entry)
        print("Produced:", entry)
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopped producing events.")
