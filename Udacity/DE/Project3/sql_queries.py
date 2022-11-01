import configparser

print (" Getting into  # sql_queries.py")
# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA = 's3://udacity-dend/log_data' #config.get("S3","LOG_DATA")
LOG_JSONPATH = 's3://udacity-dend/log_json_path.json' #config.get("S3","LOG_JSONPATH")
SONG_DATA = 's3://udacity-dend/song_data' #config.get("S3","SONG_DATA") 
ARN = 'arn:aws:iam::498940793624:role/apple' #config.get("IAM_ROLE","ARN") 
#REGION = config.get("GEO","REGION") 

# DROP TABLES

staging_events_table_drop = "drop table if exists staging_events"
staging_songs_table_drop = "drop table if exists staging_songs"
songplay_table_drop = "drop table if exists songplay"
user_table_drop = "drop table if exists users"
song_table_drop = "drop table if exists song"
artist_table_drop = "drop table if exists artist"
time_table_drop = "drop table if exists time"

# CREATE TABLES

staging_events_table_create= (""" CREATE TABLE IF NOT EXISTS staging_events(
    artist TEXT,
    auth TEXT,
    first_name TEXT,
    gender CHAR(1),
    item_session INTEGER,
    last_name TEXT,
    length NUMERIC,
    level TEXT,
    location TEXT,
    method TEXT,
    page TEXT,
    registration NUMERIC,
    session_id INTEGER,
    song TEXT,
    status INTEGER,
    ts BIGINT,
    user_agent TEXT,
    user_id INTEGER)
""")

staging_songs_table_create = """ CREATE TABLE IF NOT EXISTS staging_songs (
num_songs int,
artist_id varchar NOT NULL PRIMARY KEY,
artist_latitude decimal,
artist_longitude decimal,
artist_location varchar,
artist_name varchar,
song_id varchar,
title varchar,
duration decimal,
year int
)
"""

songplay_table_create = """
 CREATE TABLE IF NOT EXISTS songplay(
 songplay_id int IDENTITY(0,1) NOT NULL PRIMARY KEY,
    start_time timestamp NOT NULL,
    user_id varchar NOT NULL,
    level varchar ,
    song_id varchar,
    artist_id varchar,
    session_id varchar NOT NULL,
    location varchar,
    user_agent varchar 
    )
"""
# artist varchar,song varchar, length decimal, page varchar
user_table_create = """ CREATE TABLE IF NOT EXISTS users(
user_id int NOT NULL PRIMARY KEY,
first_name varchar,
last_name varchar,
gender varchar,
level varchar
)
"""

song_table_create = (""" CREATE TABLE IF NOT EXISTS songS(
song_id int NOT NULL PRIMARY KEY,
title varchar,
artist_id int NOT NULL,
year int,
duration DECIMAL
)
""")

artist_table_create = """CREATE TABLE IF NOT EXISTS artists(
artist_id int NOT NULL PRIMARY KEY,
name varchar,
location varchar,
latitude DECIMAL,
longitude DECIMAL
)
"""

time_table_create = """ CREATE TABLE IF NOT EXISTS time(
start_time varchar NOT NULL PRIMARY KEY, 
hour int NOT NULL, 
day int NOT NULL, 
week int NOT NULL, 
month int NOT NULL, 
year int NOT NULL, 
weekday int NOT NULL)
"""

# STAGING TABLES
# SQL_COPY = """
# copy {} from 's3://awssampledbuswest2/ssbgz/{}' 
# credentials 'aws_iam_role={}'
# gzip region 'us-west-2';
#         """.format(table,table, DWH_ROLE_ARN)

# Song data: s3://udacity-dend/song_data
# Log data: s3://udacity-dend/log_data
# Log data json path: s3://udacity-dend/log_json_path.json

# staging_events_copy = ("""
# copy {} from 's3://udacity-dend/song_data/'
# credentials 'aws_iam_role={}'
# region 'us-west-2'
# """).format(config.get("IAM_ROLE","ARN"))

staging_events_copy = f"""
copy staging_events from 's3://udacity-dend/song_data/'
credentials 'aws_iam_role=arn:aws:iam::498940793624:role/apple'
JSON {config.get('S3','LOG_JSONPATH')}
region 'us-west-2'
 """
#.format(config.get("IAM_ROLE","ARN"))

staging_songs_copy = f"""
copy staging_songs from 's3://udacity-dend/song_data'
credentials 'aws_iam_role=arn:aws:iam::498940793624:role/apple'
JSON {config.get('S3','SONG_DATA')}
region 'us-west-2'
"""

# FINAL TABLES

songplay_table_insert = """
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT distinct s.ts,
         e.user_id,
         e.level,
         s.song_id,
         s.artist_id,
         e.session_Id,
         e.location,
         e.user_agent
FROM staging_events AS e
JOIN staging_songs AS s
     ON  (e.artist = s.artist_name)
     WHERE e.page = 'NextSong' 
"""
 
user_table_insert = """
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT 
DISTINCT user_id, first_name, last_name, gender, level 
FROM staging_events
"""

song_table_insert = """
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT
    DISTINCT song_id,
    title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
"""

artist_table_insert = """
INSERT INTO artists(artist_id, name, location, latitude, longitude)
SELECT
    DISTINCT artist_id,
    artist_name, artist_location, artist_latitude, artist_longitude
FROM stating_songs
WHERE artist_id IS NOT NULL
"""

time_table_insert = """
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT
    DISTINCT ts,
    EXTRACT(hour FROM ts),
    EXTRACT(day FROM ts),
    EXTRACT(week FROM ts),
    EXTRACT(month FROM ts),
    EXTRACT(year FROM ts),
    EXTRACT(weekday FROM ts)
FROM staging_events
WHERE page = 'NextSong' AND ts IS NOT NULL
"""

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create,  song_table_create, artist_table_create, time_table_create,user_table_create] #user_table_create, user_table_create
drop_table_queries   = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
print (" Exiting # sql_queries.py")
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
