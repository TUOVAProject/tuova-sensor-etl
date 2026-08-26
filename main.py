from config import config as opts
from influx import get_data
from summary import get_summary
from visualization import visualize
from postgres import insert_summary


date = "2026-08-04"
filename = f"./data/{date}.csv"

###

# test the logik
def main():
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
