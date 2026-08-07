import os
from dotenv import load_dotenv
from influx import get_data
from summary import get_summary
from visualization import visualize
from postgres import insert_summary

load_dotenv()

date = "2026-08-03"
filename = f"./data/{date}.csv"

# config
opts = {
    # summary:
    "field_name": "L1", 
    "states": {
        -1: "LOW_ERR",
        0: "OFF",
        1: "IDLE",
        2: "PROCESSING",
        3: "HIGH_ERR"
    }, 
    "thresholds": [
        0,
        0.8,
        6.5,
        100
    ],
    "debounce_up_sec": 15, 
    "debounce_down_sec": 30,

    # influxdb:
    "influx_url": os.getenv("INFLUX_URL"),
    "influx_org": os.getenv("INFLUX_ORG"),
    "influx_bucket": os.getenv("INFLUX_BUCKET"),
    "influx_measurement": os.getenv("INFLUX_MEASUREMENT"),
    "influx_token": os.getenv("INFLUX_TOKEN"),

    # postgres:
    "pg_host": os.getenv("POSTGRES_HOST"),
    "pg_port": os.getenv("POSTGRES_PORT"),
    "pg_user": os.getenv("POSTGRES_USER"),
    "pg_pass": os.getenv("POSTGRES_PASS"),
    "pg_db": os.getenv("POSTGRES_DB"),
}

###

# test the logik
def main():
    in_csv = get_data(date=date, opts=opts)  # read from API
#    in_csv = filename       # read from file

#    # write to file
#    with open(filename, "w") as f:
#        f.write(in_csv.read())

    results = get_summary(in_csv=in_csv, opts=opts)
    visualize(results=results, opts=opts)
    #insert_summary(data=results["segments"], opts=opts)

    return 0

###

# this would be the whole process
def dag_process(date=None):

    # TODO: do the process

    # get data
    in_csv = get_data(date=date, opts=opts)

    # get summary
    results = get_summary(in_csv=in_csv, opts=opts)

    # format and insert to db
    insert_summary(data=results["segments"], opts=opts)
    # TODO

    return 0


###
if __name__ == "__main__":
    main()
