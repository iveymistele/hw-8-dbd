import asyncio
import websockets
import json
import duckdb
from datetime import datetime

uri = "wss://jetstream2.us-east.bsky.network/subscribe?wantedCollections=app.bsky.feed.post"

conn = duckdb.connect("bluesky.duckdb")

conn.execute("""
CREATE TABLE IF NOT EXISTS posts (
    id VARCHAR,
    did VARCHAR,
    collection VARCHAR,
    rkey VARCHAR,
    text VARCHAR,
    created_at VARCHAR,
    raw_json VARCHAR
)
""")

log_file = open("ingestion.log", "a")

def log(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    log_file.write(line + "\n")
    log_file.flush()

async def listen_to_websocket():
    async with websockets.connect(uri) as websocket:
        log("Connected to Jetstream")
        while True:
            try:
                message = await websocket.recv()
                data = json.loads(message)

                commit = data.get("commit", {})
                record = commit.get("record", {})

                did = data.get("did")
                collection = commit.get("collection")
                rkey = commit.get("rkey")
                text = record.get("text")
                created_at = record.get("createdAt")

                post_id = None
                if did and collection and rkey:
                    post_id = f"at://{did}/{collection}/{rkey}"

                if not post_id:
                    log("Skipping event: missing post_id pieces")
                    continue
                
                if not text:
                  log(f"Skipping empty-text post: {post_id}")
                  continue

                conn.execute("""
                    INSERT INTO posts (id, did, collection, rkey, text, created_at, raw_json)
                    SELECT ?, ?, ?, ?, ?, ?, ?
                    WHERE NOT EXISTS (
                        SELECT 1 FROM posts WHERE id = ?
                    )
                """, [post_id, did, collection, rkey, text, created_at, message, post_id])

                log(f"Inserted post: {post_id}")

            except websockets.ConnectionClosed as e:
                log(f"Connection closed: {e}")
                break
            except Exception as e:
                log(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(listen_to_websocket())