from dotenv import load_dotenv

load_dotenv()

import json
from tuova_sensors.config import config
from tuova_sensors.influx import get_data
from tuova_sensors.summary import get_summary
from tuova_sensors.visualization import visualize
from tuova_sensors.postgres import insert_summary


date = "2026-08-04"

# test the logik
def test():

    workcenters = json.loads(config["workcenters"])

    for w in workcenters:
        opts = config | {
            "influx_measurement": w["influx_measurement"],
            "influx_field": w["influx_field"],
            "workcenter_id": w["workcenter_id"]
        }

        in_csv = get_data(date=date, opts=opts)
        print(len(in_csv.getvalue()))

        results = get_summary(in_csv=in_csv, opts=opts)
        print(results["totals"])

        # visualize(results=results, opts=opts)

        res = insert_summary(data=results, opts=opts, dry_run=True)
        print(res)

    return 0


###
if __name__ == "__main__":
    test()
