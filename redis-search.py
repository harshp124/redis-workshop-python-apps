import os
from dotenv import load_dotenv
import redis
from redis.commands.search.field import TextField, NumericField, TagField, GeoField
from redis.commands.search.index_definition import IndexDefinition, IndexType
from redis.commands.search.query import Query
from redis.commands.search.aggregation import AggregateRequest, Asc
from redis.commands.search.query import GeoFilter
from redis.commands.search import reducers

load_dotenv()  # Loads variables from .env

def wait_for_user(command="continue"):
    input("\nPress Enter to " + command + "...")
    clear_screen()

def clear_screen():
    if os.name == 'nt':  # Windows
        os.system('cls')
    else:                # Linux, macOS, and others
        os.system('clear')

clear_screen()

# --- Redis Database Connection ---

r = redis.Redis(
    host=os.getenv('REDIS_HOST'),
    port=os.getenv('REDIS_PORT'),
    username=os.getenv('REDIS_USERNAME'),
    password=os.getenv('REDIS_PASSWORD'),
    decode_responses=True
)

# =================== RedisSearch Example ===================
print("===== RedisSearch Demo with connection info including username & password =====")
wait_for_user("start the demo")

# Step 1: Define Index schema
print("Step 1: Defining index schema with Text, Numeric, Tag, and Geo fields.")
schema = (
    TextField("title", weight=5.0),
    TextField("body"),
    NumericField("price"),
    TagField("category"),
    GeoField("location")
)

definition = IndexDefinition(prefix=["doc:"], index_type=IndexType.HASH)

# Create Search client
search = r.ft("idx:demo")  # index name

# Drop index if exists (clean slate)
try:
    print("Dropping existing index if it exists...")
    search.dropindex(delete_documents=True)
except Exception as e:
    print("No existing index to drop or error:", e)

wait_for_user("create index")

# Create the index with schema and definition
search.create_index(schema, definition=definition)
print("Index created successfully.")
wait_for_user()

# Step 2: Add documents
print("Step 2: Adding sample documents to the index.")

docs = [
    {
        "key": "doc:1",
        "fields": {
            "title": "Red Apple",
            "body": "A tasty red apple from the orchard.",
            "price": 1.50,
            "category": "fruit,food",
            "location": "-122.431297 37.773972"  # San Francisco coords: lon lat
        }
    },
    {
        "key": "doc:2",
        "fields": {
            "title": "Green Apple",
            "body": "Sour green apple for pies.",
            "price": 1.20,
            "category": "fruit,food",
            "location": "-74.005974 40.712776"  # New York coords
        }
    },
    {
        "key": "doc:3",
        "fields": {
            "title": "Red Bicycle",
            "body": "A bright red mountain bike.",
            "price": 150.00,
            "category": "sports,vehicle",
            "location": "-118.243685 34.052234"  # Los Angeles coords
        }
    },
    {
        "key": "doc:4",
        "fields": {
            "title": "Mountain Bike",
            "body": "All-terrain bike for mountain trails.",
            "price": 200.00,
            "category": "sports,vehicle",
            "location": "-122.676483 45.523064"  # Portland coords
        }
    }
]

for doc in docs:
    r.hset(doc["key"], mapping=doc["fields"])
print(f"Added {len(docs)} documents.")
wait_for_user()

# Step 3: Simple full-text search (search "red")
print("Step 3: Simple full-text search for the word 'red'.")
res = search.search("red")
print(f"Total results found: {res.total}")
for doc in res.docs:
    print(f"DocID: {doc.id}, Title: {doc.title}, Price: {doc.price}, Category: {doc.category}")

wait_for_user()

# Step 4: Numeric filtering price range 1 to 2 (cheap items)
print("Step 4: Numeric filter for price between 1 and 2.")
query = Query("@price:[1 2]")
res = search.search(query)
print(f"Total results with price between 1 and 2: {res.total}")
for doc in res.docs:
    print(f"{doc.id}: {doc.title} - ${doc.price}")

wait_for_user()

# Step 5: Tag filtering category contains 'vehicle'
print("Step 5: Tag filtering for category tags 'vehicle'.")
query = Query("@category:{vehicle}")
res = search.search(query)
print(f"Total vehicle category documents: {res.total}")
for doc in res.docs:
    print(f"{doc.id}: {doc.title} in categories {doc.category}")

wait_for_user()

# Step 6: Geo search - within 100km radius of SF (-122.431297 37.773972)
print("Step 6: Geo search within 100km radius of San Francisco.")
query = Query("*").add_filter(
    GeoFilter("location", -122.431297, 37.773972, 100, unit="km")
)
res = search.search(query)
print(f"Total documents within 100km of SF: {res.total}")
for doc in res.docs:
    print(f"{doc.id}: {doc.title} at location {doc.location}")

wait_for_user()

# Step 7: Sorting results by price asc, limit to 3 results
print("Step 7: Sorting search results by price ascending, limit 3.")
query = Query("*").sort_by("price", asc=True).paging(0,3)
res = search.search(query)
print(f"Top 3 cheapest documents:")
for doc in res.docs:
    print(f"{doc.id}: {doc.title} - ${doc.price}")

wait_for_user()


# Step 8: Aggregation - Group by category and count how many docs each category has
print("Step 8: Aggregation: Group by category and count documents each category has.")
agg_req = AggregateRequest("*") \
        .group_by("@category", reducers.count().alias("count")) \
        .sort_by(Asc("@category"))

agg_res = search.aggregate(agg_req)
for row in agg_res.rows:
    print (row)

wait_for_user()


# Step 9: Updating a document
print("Step 9: Update document doc:1 - changing price to 1.75")
r.hset("doc:1", mapping={"price": 1.75})
print("Doc:1 updated.")
wait_for_user()

# Step 10: Confirm the update
print("Step 10: Search for 'apple' and show updated price.")
res = search.search("apple")
for doc in res.docs:
    print(f"{doc.id}: {doc.title} - Price: {doc.price}")
wait_for_user()

# Step 11: Deleting a document
print("Step 11: Deleting document doc:4.")
r.delete("doc:4")
print("doc:4 deleted from Redis.")
wait_for_user()

# Step 12: Confirm deletion - search all documents
print("Step 12: Search all documents to confirm deletion of doc:4.")
res = search.search("*")
for doc in res.docs:
    print(f"{doc.id}: {doc.title}")

wait_for_user("finish the demo")

print("===== RedisSearch Demo Completed Successfully =====")
