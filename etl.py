import os
import glob
import numpy as np
import psycopg2
import json
from sql_queries import *
import pandas as pd


def process_song_file(cur, filepath):
    """
    Description: Process the song files in JSON format and populate songs & artists tables. 

    Arguments:
        cur: the cursor object
        filepath: file path for the files that need to be processed

    Returns:
        Populates the songs & artists tables tables in the sparkify database
    """
    song_file = []
    # open song file
    with open(filepath) as json_data:
        data = json.load(json_data)
        json_data.close()
        song_file.append(data)

    df = pd.DataFrame(song_file)

    # insert song record
    song_data = df.get(["song_id", "title", "artist_id", "year", "duration"]).values[0]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = df.get(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"]).values[0]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Description: Process the log files in JSON format and populate time, users & songplays tables. 

    Arguments:
        cur: the cursor object
        filepath: file path for the files that need to be processed

    Returns:
        Populates the time, users & songplays tables tables in the sparkify database
    """
    logs = []
    # open log file
    with open(filepath) as fp:
        line = fp.readline()
        line = line.strip()
        if len(line) > 0:
            json_log = json.loads(line)
            logs.append(json_log)
        while line:
            line = fp.readline()
            line = line.strip()
            if len(line) > 0:
                json_log = json.loads(line)
                logs.append(json_log)
                
    df = pd.DataFrame(logs)

    # filter by NextSong action
    filtered_df = df.loc[df['page'].isin(["NextSong"])]
    ts_vals = filtered_df.get(["ts"]).values

    # convert timestamp column to datetime
    time_list = []

    for ts_val in ts_vals:
        dt = pd.to_datetime(ts_val[0], unit='ms')
        dt_dict = {"start_time": dt,        \
                   "hour": dt.hour,         \
                   "day": dt.day,           \
                   "week": dt.weekofyear,   \
                   "month": dt.month,       \
                   "year": dt.year,         \
                   "weekday": dt.weekday()  \
                  }
        time_list.append(dt_dict)
    
    # insert time data records
    time_df = pd.DataFrame(time_list)

    for ts_data in time_df.itertuples():
        time_list = [ts_data.start_time, ts_data.hour, ts_data.day, ts_data.week, ts_data.month, ts_data.year, ts_data.weekday]
        cur.execute(time_table_insert, time_list)

    # load user table
    user_vals = df.get(["userId", "firstName", "lastName", "gender", "level"]).values

    user_list = []

    for user_val in user_vals:
        user_dict = {"user_id": user_val[0],    \
                     "first_name": user_val[1], \
                     "last_name": user_val[2],  \
                     "gender": user_val[3],     \
                     "level": user_val[4]       \
                    }
        user_list.append(user_dict)

    user_df = pd.DataFrame(user_list)

    # insert user records
    for user_data in user_df.itertuples():
        user_list = [user_data.user_id, user_data.first_name, user_data.last_name, user_data.gender, user_data.level]
        cur.execute(user_table_insert, user_list)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchall()
        if len(results) > 0:
            songid, artistid = results[0]
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (index,                                      \
                         pd.to_datetime(row.ts, unit='ms'),          \
                         row.userId,                                 \
                         row.level,                                  \
                         songid,                                     \
                         artistid,                                   \
                         row.sessionId,                              \
                         row.location,                               \
                         row.userAgent                               \
                        )
        cur.execute(songplay_table_insert, songplay_data)
        

def process_data(cur, conn, filepath, func):
    """
    Description: Process the given filepath as per the helper function passed

    Arguments:
        cur: the cursor object
        conn: connection to the database
        filepath: file path for the files that need to be processed
        func: function that should be called given the nature of the data

    Returns:
        None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Description: ETL Script for Project 1

    Arguments:
        None

    Returns:
        None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()