import io
import csv
from datetime import datetime, timedelta, timezone
import influxdb_client

# logik
def get_data(date=None, opts={}):
    influx_url = opts["influx_url"]
    influx_org = opts["influx_org"]
    influx_bucket = opts["influx_bucket"]
    influx_measurement = opts["influx_measurement"]
    influx_token = opts["influx_token"]

    if date is None:
        # default to yesterday
        date = (datetime.now(timezone.utc).astimezone() - timedelta(days=1)).date().isoformat()

    date = datetime.strptime(date, "%Y-%m-%d").replace(tzinfo=timezone.utc).astimezone()
    
    # get start and end timestamps
    start_dt = date.replace(hour=0, minute=0, second=0, microsecond=0)
    end_dt = start_dt + timedelta(days=1)

    # get data by date
    # query as "csv"
    influx = influxdb_client.InfluxDBClient(url=influx_url, org=influx_org, token=influx_token, verify_ssl=False)
    influx_query_api = influx.query_api()
    influx_query = f'from(bucket:"{influx_bucket}")\
        |> range(start: {start_dt.isoformat()}, stop:{end_dt.isoformat()}) \
        |> filter(fn:(r) => r._measurement == "{influx_measurement}" and r._field == "L1") \
        |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")'
    res = influx_query_api.query_csv(org=influx_org, query=influx_query)

    # convert to real csv
    csv_buf = io.StringIO()
    writer = csv.writer(csv_buf)
    
    # now this looks stupid but gets rid of the annotation rows
    writer.writerows(
        row for row in res
        if row and not row[0].startswith("#")
    )
    
    # reset pointer and return buffer
    csv_buf.seek(0)
    return csv_buf
