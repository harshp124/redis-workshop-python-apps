import redis
import os
from dotenv import load_dotenv

load_dotenv()  # Loads variables from .env

# --- Helper functions for pausing and clearing the terminal ---
def wait_for_user(command="continue"):
    input("\nPress Enter to " + command + "...")
    clear_screen()

def clear_screen():
    if os.name == 'nt':  # Windows
        os.system('cls')
    else:                # Linux, macOS, and others
        os.system('clear')

clear_screen()  # Clear at the very start

r = redis.Redis(
    host=os.getenv('REDIS_HOST'),
    port=os.getenv('REDIS_PORT'),
    username=os.getenv('REDIS_USERNAME'),
    password=os.getenv('REDIS_PASSWORD'),
    decode_responses=True
)

nested_json = {
    "user": {
        "id": 1002,
        "name": "Alice",
        "address": {
            "city": "New York",
            "zip": "10001"
        },
        "contacts": [
            {"type": "email", "value": "alice@example.com"},
            {"type": "phone", "value": "555-1234"}
        ],
        "stats": {
            "visits": 34,
            "is_active": True
        }
    }
}

print("=== 1. Storing nested JSON as key 'user:1002' in Redis ===")
wait_for_user("store the JSON")
r.json().set('user:1002', '$', nested_json)
print("JSON document stored.")
wait_for_user()

print("=== 2. Fetching the user's city (nested field: user.address.city) ===")
wait_for_user("fetch the city")
city = r.json().get('user:1002', '$.user.address.city')
print("City result:", city)
print("City value (first item):", city[0][0] if isinstance(city, list) and city and city[0] else city)
wait_for_user()

print("=== 3. Incrementing the visits count (user.stats.visits += 1) ===")
wait_for_user("increment visits")
new_visits = r.json().numincrby('user:1002', '$.user.stats.visits', 1)
print("Visits field after increment:", new_visits)
wait_for_user()

print("=== 4. Fetching email contacts (filtering within contacts array) ===")
wait_for_user("fetch email contacts")
email_contacts = r.json().get('user:1002', '$.user.contacts[?(@.type=="email")]')
print("Email contacts result:", email_contacts)
wait_for_user()

print("=== 5. Updating the user's city to 'San Francisco' ===")
wait_for_user("update the city")
r.json().set('user:1002', '$.user.address.city', "San Francisco")
print("City updated.")
wait_for_user()

print("=== 6. Fetching the entire updated user JSON ===")
wait_for_user("fetch the complete JSON")
updated_json = r.json().get('user:1002', '$')
print("Updated user JSON:")
print(updated_json)
wait_for_user("finish")
