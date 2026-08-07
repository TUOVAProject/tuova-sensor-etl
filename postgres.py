import psycopg

def insert_summary(data=None, opts={}):
    pg_host = opts["pg_host"]
    pg_port = opts["pg_port"]
    pg_user = opts["pg_user"]
    pg_pass = opts["pg_pass"]
    pg_db = opts["pg_db"]

    if data is None:
        return -1
    
    # create tuples as (state, start, end, duration_sec)
    tuples = list(data.itertuples(index=False, name=None))
    print(data)

    # batch insert
    with psycopg.connect(conninfo=f"host={pg_host} port={pg_port} user={pg_user} password={pg_pass} dbname={pg_db}") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM fact_machine_events")
            # TODO
            print(cur.fetchall())

    