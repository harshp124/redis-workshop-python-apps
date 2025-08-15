import redis
import os
from dotenv import load_dotenv

# Connect to Redis with username and password authentication
load_dotenv()  # Loads variables from .env

r = redis.Redis(
    host=os.getenv('REDIS_HOST'),
    port=os.getenv('REDIS_PORT'),
    username=os.getenv('REDIS_USERNAME'), 
    password=os.getenv('REDIS_PASSWORD'),
    decode_responses=True
)

def wait_for_user(command="continue"):
    input("\nPress Enter to "+ command +"...\n")
    clear_screen()

def clear_screen():
    if os.name == 'nt':  # Windows
        os.system('cls')
    else:                # Linux, macOS, and others
        os.system('clear')

def demo_string():
    print("=== Redis STRING Demo ===")
    print("We'll SET a string key 'greeting' with a message - 'Hello, Redis!'")
    wait_for_user("insert the string in Redis")
    r.set('greeting', 'Hello, Redis!')
    print("String inserted into Redis")
    
    print("\n\n\n\nNow we GET the full value of key 'greeting' from Redis DB")
    wait_for_user("Get the string from Redis DB")
    full_value = r.get('greeting')
    print(f"\n\n\n\ngreeting: {full_value}")
    wait_for_user("Go to the next part - Redis HASH")

def demo_hash():
    print("\n\n\n\n=== Redis HASH Demo ===")
    print("We'll create a user profile with id 'user:1001' storing multiple fields as follows\n")
    print("name':'Alice, 'email': 'alice@example.com', 'age': '29','country': 'Wonderland'\n")
    wait_for_user("insert this hash into Redis")
    r.hset('user:1001', mapping={
        'name': 'Alice',
        'email': 'alice@example.com',
        'age': '29',
        'country': 'Wonderland'
    })
    print("\n\n\n\n\nHash with key user:1001 inserted into Redis")
    wait_for_user("now Fetch this whole Hash from Redis DB")

    print("\n\n\n\nGET the entire hash for 'user:1001':")
    user_profile = r.hgetall('user:1001')
    for k, v in user_profile.items():
        print(f"{k}: {v}")
    
    wait_for_user("Fetch only specific field (Email) from the Hash")
    print("\n\n\n\n\nFetching specific field — Email:")
    email = r.hget('user:1001', 'email')
    print(f"Email: {email}")
    wait_for_user("Go to the next part - Redis LIST")

def demo_list():
    print("\n\n\n\n=== Redis LIST Demo ===")
    print("We'll store recent search queries on a website in a list called 'searches'.\n")
    print("Here are the values we will store in the list\n ['redis tutorial', 'python redis', 'data structures', 'redis commands']")
    wait_for_user("Insert this List into Redis")
    searches = ['redis tutorial', 'python redis', 'data structures', 'redis commands']
    r.delete('searches')  # Clear existing list if any
    for item in searches:
        r.rpush('searches', item)
    print("List inserted into Redis\n")
    wait_for_user("now GET full list 'searches'")

    print("\n\n\n\n\nGET full list 'searches':")
    all_searches = r.lrange('searches', 0, -1)
    print(all_searches)

    print("\n\n\n\nNext we will fetch specific element by index (1) from this list 'searches'")
    wait_for_user()
    second_search = r.lindex('searches', 1)
    print(f"List element with Index 1: '{second_search}'")
    wait_for_user("Go to the next part - Redis SETs")

def demo_set():
    print("\n\n\n\n=== Redis SET Demo ===")
    print("Let's store unique tags under 'post:tags', and try to add below values")
    print("['python', 'redis', 'database', 'nosql', 'redis']")
    print("\n\nNotice we have a duplicate tags here named 'redis'")
    print("So, when we insert this tags into the set, it would automatically prevent adding suplicate values")
    print("\n\nLets add this set into Redis and get the set to view the results")
    wait_for_user("Add 'post:tags' to Redis")
    tags = ['python', 'redis', 'database', 'nosql', 'redis']  # Redis sets prevent duplicates
    r.delete('post:tags')
    for tag in tags:
        r.sadd('post:tags', tag)
    print("\n\n\n\n\nSet added to Redis.")
    wait_for_user("Fetch the Set 'post:tags'")

    print("\n\n\n\nShow the SET 'post:tags':")
    all_tags = r.smembers('post:tags')
    print(all_tags)

    wait_for_user()

    print("\n\n\n\n\nNow lets check if 'redis' is a tag using 'sismember' method")
    wait_for_user()
    has_redis = r.sismember('post:tags', 'redis')
    print(f"'redis' is a tag: {has_redis}")
    wait_for_user("Go to the next part - Redis SORTED SETs")

def demo_sorted_set():
    print("\n\n\n\n\n=== Redis SORTED SET Demo ===")
    print("Leaderboard under 'game:leaderboard' with player scores.")
    print("Below is what we would add to our Sorted Set Leaderboard with key 'game:leaderboard'")
    print("Alice=1500, Bob =1800, Clara=1200, Dave =2000")
    wait_for_user("add the 'game:leaderboard sorted set into our Redis DB'")
    leaderboard = {
        'Alice': 1500,
        'Bob': 1800,
        'Clara': 1200,
        'Dave': 2000
    }
    r.delete('game:leaderboard')
    for player, score in leaderboard.items():
        r.zadd('game:leaderboard', {player: score})
    print("\n\n\n\n\n'game:leaderboard' added to Redis DB")
    wait_for_user()

    print("\n\n\n\n\n Now lets View our Full leaderboard (highest to lowest):")
    wait_for_user("View the Leaderboard")
    board = r.zrevrange('game:leaderboard', 0, -1, withscores=True)
    for rank, (player, score) in enumerate(board, start=1):
        print(f"{rank}. {player} - {score}")

    wait_for_user()
    print("\n\n\n\n\nNow lets get specific scores from the leaderboard")
    wait_for_user("Get score for 'Clara'")
    clara_score = r.zscore('game:leaderboard', 'Clara')
    print(f"\n\n\n\n\nClara's score: {clara_score}")
    wait_for_user()

def demo_bitmap():
    print("=== Redis BITMAP Demo ===")
    print("We'll track attendance for 7 days for a single user with key 'user:attendance'.")
    r.delete('user:attendance')
    # Mark days 0, 2, 6 as present
    present_days = [0, 2, 6]
    for day in present_days:
        r.setbit('user:attendance', day, 1)
    wait_for_user()

    print("GET attendance bitmap bits for days 0 to 6:")
    attendance = [r.getbit('user:attendance', day) for day in range(7)]
    print(f"Attendance bits: {attendance}")

    print("\nCheck if user was present on day 3 (0-based):")
    day3 = r.getbit('user:attendance', 3)
    print(f"Day 3 present? {'Yes' if day3 else 'No'}")
    wait_for_user()

def demo_hyperloglog():
    print("=== Redis HYPERLOGLOG Demo ===")
    print("Let's approximate the unique visitors on a website.")
    r.delete('unique_visitors')
    visitors = ['user1', 'user2', 'user3', 'user2', 'user4', 'user1', 'user5']
    for visitor in visitors:
        r.pfadd('unique_visitors', visitor)
    wait_for_user()

    print("Approximate unique visitor count:")
    count = r.pfcount('unique_visitors')
    print(f"Unique visitors: {count}")
    wait_for_user()

def main():
    print("Welcome to Redis Data Structures Demo!\n")
    demo_string()
    demo_hash()
    demo_list()
    demo_set()
    demo_sorted_set()
    # demo_bitmap()
    # demo_hyperloglog()
    print("Demo complete. Thanks for learning Redis with Python!")

if __name__ == '__main__':
    main()
