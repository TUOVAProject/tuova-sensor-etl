import os
import psycopg

#env
pg_host = os.getenv("POSTGRES_HOST")
pg_port = os.getenv("POSTGRES_PORT")
pg_user = os.getenv("POSTGRES_USER")
pg_pass = os.getenv("POSTGRES_PASS")
pg_db = os.getenv("POSTGRES_DB")

def insert_summary(segments):
    # create tuples as (segment_id, start, end, duration_sec)
    tuples = list(segments.itertuples(index=False, name=None))
    print(segments)

    # batch insert
    with psycopg.connect(conninfo=f"host={pg_host} port={pg_port} user={pg_user} password={pg_pass} dbname={pg_db}") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM fact_machine_events")
            print(cur.fetchall())

    