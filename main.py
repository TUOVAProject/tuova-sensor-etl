from data import get_data
from summary import get_summary
from vis import visualize

date = "2026-08-03"
filename = f"./data/{date}.csv"

# config
opts = {
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
        1,
        6.5,
        100
    ],
    "debounce_up_sec": 15, 
    "debounce_down_sec": 30
}

###

# test the logik
def main():
    in_csv = get_data(date)  # read from API
#    in_csv = filename       # read from file

#    # write to file
#    with open(filename, "w") as f:
#        f.write(in_csv.read())

    results = get_summary(opts=opts, in_csv=in_csv)
    df = results["segments"]
    visualize(opts=opts, results=results)

    return 0


# this would be the whole process
def dag_process(date=None):

    # TODO: do the process

    # get data
    in_csv = get_data(date)

    # get summary
    results = get_summary(opts=opts, in_csv=in_csv)

    # format and insert to db
    # TODO

    return 0


###
if __name__ == "__main__":
    main()
