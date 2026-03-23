import duckdb

conn = duckdb.connect("bluesky.duckdb")

conn.execute("""
COPY posts TO 'posts.csv' (HEADER, DELIMITER ',');
""")