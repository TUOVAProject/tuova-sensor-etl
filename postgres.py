import psycopg

def insert_summary(data=None, opts={}):
    pg_host = opts["pg_host"]
    pg_port = opts["pg_port"]
    pg_user = opts["pg_user"]
    pg_pass = opts["pg_pass"]
    pg_db = opts["pg_db"]

    event_type_ids = opts["event_type_ids"]
    event_source_id = opts["event_source_id"]
    event_workcenter_id = opts["event_workcenter_id"]

    if data is None:
        return -1
    
    # create segment tuples as (state, start, end, duration_sec)
    tuples = list(data["segments"].itertuples(index=False, name=None))

    # create row from each with (event_type_id, source_id, workcenter_id, start_at, end_at, duration_min)
    rows = []
    for t in tuples:
        rows.append((
            event_type_ids[t[0]],
            event_source_id,
            int(event_workcenter_id),
            t[1].tz_localize("Europe/Helsinki").isoformat(),
            t[2].tz_localize("Europe/Helsinki").isoformat(),
            float(t[3] / 60)
            ))

    # TODO: daily summary from data["totals"]

    # batch insert
    with psycopg.connect(conninfo=f"host={pg_host} port={pg_port} user={pg_user} password={pg_pass} dbname={pg_db}") as conn:
        with conn.cursor() as cur:
            cur.executemany("""
                INSERT INTO fact_machine_events (event_type_id, source_id, workcenter_id, start_at, end_at, duration_min) 
                VALUES (%s, %s, %s, %s, %s, %s);
            """, rows)
