import psycopg
from datetime import timedelta

def insert_summary(data=None, opts={}, dry_run=False):
    pg_host = opts["pg_host"]
    pg_port = opts["pg_port"]
    pg_user = opts["pg_user"]
    pg_pass = opts["pg_pass"]
    pg_db = opts["pg_db"]

    event_type_ids = opts["event_type_ids"]
    event_source_id = opts["event_source_id"]
    workcenter_id = opts["workcenter_id"]

    LOCALE = opts["LOCALE"]

    if data is None or data["segments"] is None or len(data["segments"]) <= 0:
        return -1
    
    # create segment tuples as (state, start, end, duration_sec)
    tuples = list(data["segments"].itertuples(index=False, name=None))

    # create row from each with (event_type_id, source_id, workcenter_id, start_at, end_at, duration_min, efficient_production_min, efficient_production_pct, idle__sum_min, idle_sum_pct, power_off_min, power_off_pct)
    rows = []

    # daily summary row from data["totals"]
    t_tuples = list(data["totals"].itertuples(index=False, name=None))
    start_of_day = tuples[0][1].replace(hour=0,minute=0,second=0,microsecond=0).tz_localize(LOCALE)
    end_of_day = start_of_day + timedelta(days=1)
    minutes_in_day = (end_of_day - start_of_day).total_seconds() / 60 # might as well just say 1440 here but we live in strange times
    # TODO: should account for other states too, i.e. get these from opts[]
    processing_min = next((t[0] for t in t_tuples if t[2] == "PROCESSING"), 0) / 60
    idle_min = next((t[0] for t in t_tuples if t[2] == "IDLE"), 0) / 60
    off_min = next((t[0] for t in t_tuples if t[2] == "OFF"), 0) / 60

    # summary row
    rows.append((
        event_type_ids[9999],
        event_source_id,
        int(workcenter_id),
        start_of_day.isoformat(),
        end_of_day.isoformat(),
        minutes_in_day,
        processing_min,
        processing_min / minutes_in_day,
        idle_min,
        idle_min / minutes_in_day,
        off_min,
        off_min / minutes_in_day,
        int(start_of_day.date().strftime("%Y%m%d")),
        int(end_of_day.date().strftime("%Y%m%d")),
        int(start_of_day.time().strftime("%H%M%S")),
        int(end_of_day.time().strftime("%H%M%S"))
    ))

    # segment rows
    for t in tuples:
        processing_min = float(t[3] / 60) if opts["states"][t[0]] == "PROCESSING" else 0
        idle_min = float(t[3] / 60) if opts["states"][t[0]] == "IDLE" else 0
        off_min = float(t[3] / 60) if opts["states"][t[0]] == "OFF" else 0
        start_t = t[1].tz_localize(LOCALE)
        end_t = t[2].tz_localize(LOCALE)

        rows.append((
            event_type_ids[t[0]],
            event_source_id,
            int(workcenter_id),
            start_t.isoformat(),
            end_t.isoformat(),
            float(t[3] / 60),
            processing_min,
            processing_min / float(t[3] / 60),
            idle_min,
            idle_min / float(t[3] / 60),
            off_min,
            off_min / float(t[3] / 60),
            int(start_t.date().strftime("%Y%m%d")),
            int(end_t.date().strftime("%Y%m%d")),
            int(start_t.time().strftime("%H%M%S")),
            int(end_t.time().strftime("%H%M%S"))
        ))

    # to csv
    '''
    with open("./data/summary/test.csv", "w") as f:
        f.write("event_type_id,source_id,workcenter_id,start_at,end_at,duration_min,efficient_production_min,efficient_production_pct,idle_sum_min,idle_sum_pct,power_off_min,power_off_pct\n")
        for r in rows:
            f.write(",".join(str(s) for s in r))
            f.write("\n")
    '''

    # batch insert
    if dry_run is not True:
        with psycopg.connect(conninfo=f"host={pg_host} port={pg_port} user={pg_user} password={pg_pass} dbname={pg_db}") as conn:
            with conn.cursor() as cur:
                cur.executemany("""
                    INSERT INTO fact_machine_events (
                        event_type_id,
                        source_id,
                        workcenter_id,
                        start_at,
                        end_at,
                        duration_min,
                        efficient_production_min,
                        efficient_production_pct,
                        idle_sum_min,
                        idle_sum_pct,
                        power_off_min,
                        power_off_pct,
                        start_date,
                        end_date,
                        start_time,
                        end_time
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, rows)

    else:
        print(f"Pretending to insert {len(rows)} rows...")
        # print the daily summary row + first event
        print(rows[0])
        print()
        print(rows[1])
