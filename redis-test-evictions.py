import redis
import os
from dotenv import load_dotenv

load_dotenv()  # Loads variables from .env

def clear_screen():
    if os.name == 'nt':  # Windows
        os.system('cls')
    else:                # Linux, macOS, others
        os.system('clear')

def press_enter_to_continue():
    input("\nPress Enter to continue...\n")

def generate_value(size_bytes):
    # Generate a string value of roughly size_bytes length by repeating a pattern
    chunk = "abcdefghij"  # 10 chars
    repeat_times = size_bytes // len(chunk)
    remainder = size_bytes % len(chunk)
    return (chunk * repeat_times) + chunk[:remainder]

def main():
    clear_screen()
    print("\nPlease NOTE - Before you start this Lab, make sure your Redis Database's 'Data eviction policy' (in 'Durability Section') has been set to 'allkeys-lru'")
    press_enter_to_continue()
    clear_screen()
    print("Connecting to Redis database...")

    r = redis.Redis(
        host = os.getenv('REDIS_HOST'),
        port = os.getenv('REDIS_PORT'),
        username = os.getenv('REDIS_USERNAME'),
        password = os.getenv('REDIS_PASSWORD'),
        decode_responses=True
    )
    print("Connected to Redis Database.")
    print("Next, Lets flush the database before we run this Lab")
    press_enter_to_continue()
    
    # Flush the database to clear all keys and free memory
    r.flushdb()

    clear_screen()
    print("Redis database flushed.")
    print("Now lets start inserting keys into the Redis Database, Once you press enter below, switch to Redis Database Monitoring and monitor below metrics")
    print("'Used memory' AND 'Evicted objects/sec'")
    press_enter_to_continue()
    clear_screen()

    value_size = 320 * 1024  # 320 KB = 327,680 bytes

    clear_screen()
    print(f"Inserting 3000 keys with values of approximately {value_size} bytes each...")

    for i in range(1, 3001):
        key = f"key:{i}"
        value = generate_value(value_size)
        r.set(key, value)
        if i % 100 == 0:
            print(f"Inserted {i} keys...")

    print("Insertion complete.")
    print("Monitoring the 'Used memory' and 'Evicted objects/sec' metrics, you would have observed that once the database is full, \nRedis automatically starts evicting least recently used keys from the Database to make space for the incoming inserts")
    print("That is visible by observing that the 'Evicted objects/sec' metrics starts once the Database is full")
    print("\n===== This concludes our Test Evition Policy Lab =====")

if __name__ == "__main__":
    main()
