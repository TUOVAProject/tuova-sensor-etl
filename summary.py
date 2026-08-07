import pandas as pd
import numpy as np

def get_summary(in_csv=None, in_df=None, opts={}):
    # config
    field_name = opts["field_name"]
    debounce_up_sec = opts["debounce_up_sec"]
    debounce_down_sec = opts["debounce_down_sec"]

    df: pd.DataFrame = None

    # accepts csv buffer or file path
    if in_csv is not None:
        try:
            df = pd.read_csv(in_csv)

        except Exception as e:
            print("read csv - err: " + str(e))
            return None
        
    # or a dataframe
    if in_df is not None:
        df = in_df

    # parse timestamps to "ts"
    df["ts"] = pd.to_datetime(df["_time"], format="mixed", utc=True).dt.tz_convert("Europe/Helsinki").dt.tz_localize(None)

    # use ts and FIELD_NAME only (could be "L1" or whatever is used)
    df = df[["ts", field_name]]
    df = df.set_index("ts")

    # resample to get even 1s intervals
    df = df.resample("1s").mean()

    # add smoothing
    df["smoothed"] = df[field_name].rolling("5s").mean()

    # add states
    thresholds = [
        (df["smoothed"] < opts["thresholds"][0]),
        (df["smoothed"] < opts["thresholds"][1]),
        (df["smoothed"] < opts["thresholds"][2]),
        (df["smoothed"] < opts["thresholds"][3]),
        (df["smoothed"] >= opts["thresholds"][3]),
    ]

    # values assigned by state
    # ["LOW_ERR", "OFF", "IDLE", "PROCESSING", "HIGH_ERR"]
    values = [-1, 0, 1, 2, 3]

    df["state"] = np.select(thresholds, values)

    # handle short events ("PARAMETERIZED ASYMMETRIC DEBOUNCE")™
    # e.g. if (going from IDLE to PROCESSING and duration of PROCESSING < 10s), count as IDLE (removes short spikes from IDLE time)
    # likewise if (going from PROCESSING to IDLE and duration of IDLE < 30s), count as PROCESSING (removes short gaps from PROCESSING time)
    # uses the first one for any other transitions (i.e. to/from *_ERR and OFF states)
    states = df["state"].astype("Int64")
    change = states != states.shift()
    groups = change.cumsum()
    dur = df.groupby(groups).apply(lambda g: g.index[-1] - g.index[0])
    dur_map = groups.map(dur)
    cur_state = df.groupby(groups)["state"].first()
    prev_state = cur_state.shift()
    prev_state = prev_state.fillna(cur_state)
    # should probably have a comment here but no idea what's going on anymore
    st_thresholds = pd.Series(pd.Timedelta(seconds=debounce_up_sec), index=cur_state.index)
    st_thresholds[(prev_state == 1) & (cur_state == 2)] = pd.Timedelta(seconds=debounce_up_sec)
    st_thresholds[(prev_state == 2) & (cur_state == 1)] = pd.Timedelta(seconds=debounce_down_sec)
    st_threshold_map = groups.map(st_thresholds)
    valid = dur_map >= st_threshold_map
    df["filtered_state"] = states.where(valid).ffill()
    df.loc[df.index[0], 'filtered_state'] = df.loc[df.index[0], 'state']

    # anyway, we end up with a cleaned "state" for each datapoint
    df["state"] = df["filtered_state"]
    df = df.drop(columns=["filtered_state"])

    # this would have the "group/segment id" which represents a single discrete "event"
    # i.e. subsequent datapoints with the same state will have the same id (so they're part of the same "event")
    df["segment_id"] = df["state"].ne(df["state"].shift()).cumsum()

    # these "events" end up here with a start and end time + duration
    segments = df.groupby("segment_id").agg(
        state=("state", "first"),
        start=("state", lambda x: x.index[0]),
        end=("state", lambda x: x.index[-1]),
        duration_sec=("state", "size"),
    )

    # fix continuity (set the end time to the start of the next "event", otherwise we'll have 1s gaps in between)
    segments["end"] = segments["start"] + pd.to_timedelta(
        segments["duration_sec"], unit="s"
    )

    # get total durations in seconds and minutes
    totals = segments.groupby("state").sum("duration_sec")
    totals["duration_min"] = totals["duration_sec"].div(60)

    # add state names
    totals["state_name"] = totals.index.map(opts["states"])

    # return timeseries values, state segments ("events") and total durations
    return {"df": df, "segments": segments, "totals": totals}
