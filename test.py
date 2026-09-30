import json
from tuova_sensors.influx import get_data
from tuova_sensors.summary import get_summary
from tuova_sensors.visualization import visualize
from tuova_sensors.postgres import insert_summary


date = None

# test the logik
def test(date=None, config_path="./config.json"):

    # read config
    config = None
    with open(config_path, "r") as f:
        config = json.loads(f.read())

    if config == None:
        print("no config")
        return -1

    workcenters = config["workcenters"]

    for w in workcenters:
        opts = config | {
            "influx_measurement": w["influx_measurement"],
            "influx_field": w["influx_field"],
            "workcenter_id": w["workcenter_id"],
            "states": w["states"],
            "debounce_up_sec": w["debounce_up_sec"],
            "debounce_down_sec": w["debounce_down_sec"]
        }

        #print(json.dumps(opts))

        in_csv = get_data(date=date, opts=opts)
        print(len(in_csv.getvalue()))

        results = get_summary(in_csv=in_csv, opts=opts)
        print(results["totals"])

        visualize(results=results, opts=opts)

        #res = insert_summary(data=results, opts=opts, dry_run=True)
        #print(res)

    return 0


###
if __name__ == "__main__":
    test(date=date)
