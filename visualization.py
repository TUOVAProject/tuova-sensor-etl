from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

state_colors = {
    -1: "white",
    0: "lightgray",
    1: "yellow",
    2: "green",
    3: "white",
}

def visualize(opts=None, results=None):
    if opts is None or results is None:
        print("vis err: missing parameters")
        return -1
    
    df: pd.DataFrame = results["df"]
    segments: pd.DataFrame = results["segments"]
    totals: pd.DataFrame = results["totals"]

    # timeseries, bars and table
    fig, (a1, a2, ax_tbl) = plt.subplots(
        3, 1, sharex=True, gridspec_kw={"height_ratios": [3, 2, 1]}
    )
    
    # timeseries plot
    a1.plot(mdates.date2num(df.index), df["smoothed"])

    # bar plot
    a2.xaxis_date()
    for _, row in segments.iterrows():
        start = mdates.date2num(row.start)
        duration = row.duration_sec / 86400

        a2.broken_barh([(start, duration)], (0, 1), facecolors=state_colors[row.state])

    plt.gcf().autofmt_xdate()

    # table plot
    ax_tbl.axis("off")

    # info for states OFF, IDLE and PROCESSING
    # event counts
    count_off = (segments["state"] == 0).sum()
    count_idle = (segments["state"] == 1).sum()
    count_processing = (segments["state"] == 2).sum()

    # total durations
    dur_off = next(iter(totals.loc[totals["state_name"] == "OFF", "duration_min"]), 0)
    dur_idle = next(iter(totals.loc[totals["state_name"] == "IDLE", "duration_min"]), 0)
    dur_processing = next(iter(totals.loc[totals["state_name"] == "PROCESSING", "duration_min"]), 0)

    # percentages
    dur_total = dur_off + dur_idle + dur_processing
    per_off = dur_off / dur_total * 100
    per_idle = dur_idle / dur_total * 100
    per_processing = dur_processing / dur_total * 100

    # construct table
    data=[[(f'{count_off} times'), f'{count_idle} times', f'{count_processing} times'],
        [f'{dur_off:.2f} minutes', f'{dur_idle:.2f} minutes', f'{dur_processing:.2f} minutes'],
        [f'{per_off:.2f} %', f'{per_idle:.2f} %', f'{per_processing:.2f} %']
    ]

    cols=["OFF", "IDLE", "PROCESSING"]
    ax_tbl.table(cellText=data, colLabels=cols,loc="center", colColours=[state_colors[0], state_colors[1], state_colors[2]])

    # set display params
    a1.tick_params(labelbottom=True)
    a2.tick_params(labelbottom=True)
    a1.set_ylim(0, 50)
    a1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    title = datetime.strftime(df.index[0].to_pydatetime(), "%d.%m.%Y")
    fig.suptitle(f'{title}\nt={",".join(map(str, opts["thresholds"]))}\nidle_t={opts["debounce_down_sec"]}s, proc_t={opts["debounce_up_sec"]}s')
    plt.tight_layout()

    plt.show()
