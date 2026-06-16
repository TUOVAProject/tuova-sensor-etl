from data import get_data
from summary import get_summary
from vis import visualize

date= "2026-06-15"

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
    "debounce_up_sec": 10, 
    "debounce_down_sec": 30
}

###

# test the logik
def main():
    in_csv = get_data(date)
    results = get_summary(opts=opts, in_csv=in_csv)
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
