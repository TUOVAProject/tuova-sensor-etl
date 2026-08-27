import sys

from tuova_sensors.config import config as opts
from tuova_sensors.influx import get_data
from tuova_sensors.summary import get_summary
from tuova_sensors.visualization import visualize
from tuova_sensors.postgres import insert_summary


date = "2026-08-04"
filename = f"./data/{date}.csv"

###

# test the logik
def test():
#    in_csv = get_data(date=date, opts=opts)  # read from API
    in_csv = filename       # read from file

#    # write to file
#    with open(filename, "w") as f:
#        f.write(in_csv.read())

    results = get_summary(in_csv=in_csv, opts=opts)
    visualize(results=results, opts=opts)
    #insert_summary(data=results, opts=opts)

    return 0


###
if __name__ == "__main__":
    test()
