import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config['IAM_ROLE']['ARN']
SONG_DATA = config['S3']['SONG_DATA']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS StagEvents"
staging_songs_table_drop = "DROP TABLE IF EXISTS StagSongs"
songplay_table_drop = "DROP TABLE IF EXISTS FactSongPlays"
user_table_drop = "DROP TABLE IF EXISTS DimUsers"
song_table_drop = "DROP TABLE IF EXISTS DimSongs"
artist_table_drop = "DROP TABLE IF EXISTS DimArtists"
time_table_drop = "DROP TABLE IF EXISTS DimTime"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS StagEvents (
artist        varchar,
auth          varchar,
firstName     varchar,
gender        varchar,
itemInSession int,
lastName      varchar,
length        float,
level         varchar,
location      varchar,
method        varchar,
page          varchar,
registration  float,
sessionId     int,
song          varchar,
stats         int,
ts            timestamp,
userAgent     varchar,
userId        int)
diststyle even;
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS StagSongs (
num_songs        int,
artist_id        varchar,
artist_latitude  float,
artist_longitude float,
artist_location  varchar,
artist_name      varchar,
song_id          varchar,
title            varchar,
duration         float,
year             smallint)
diststyle even;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS FactSongPlays
(songplay_id bigint IDENTITY(0,1) PRIMARY KEY, 
start_time   timestamp,
user_id      int, 
level        varchar, 
song_id      varchar,
artist_id    varchar, 
session_id   int, 
location     varchar,
user_agent   varchar)
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS DimUsers 
(user_id     int PRIMARY KEY, 
first_name   varchar, 
last_name    varchar,
gender       varchar, 
level        varchar)
diststyle all;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS DimSongs 
(song_id     varchar     PRIMARY KEY,
title        varchar     NOT NULL,
artist_id    varchar     NOT NULL,
year         smallint    NOT NULL,
duration     float       NOT NULL)
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS DimArtists 
(artist_id   varchar     PRIMARY KEY, 
name         varchar     NOT NULL, 
location     varchar,
latitude     float, 
longitude    float)
diststyle all;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS DimTime 
(start_time  timestamp PRIMARY KEY, 
hour         smallint NOT NULL, 
day          smallint NOT NULL,
week         smallint NOT NULL, 
month        smallint NOT NULL, 
year         smallint NOT NULL, 
weekday      smallint NOT NULL)
diststyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
copy StagEvents from {}
iam_role {}
region 'us-west-2'
JSON {}
timeformat as 'epochmillisecs';
""").format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
copy StagSongs from {}
iam_role {}
json 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES

user_table_insert = ("""
INSERT INTO DimUsers (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userid, firstname, lastname, gender, level
FROM StagEvents
WHERE userid IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO DimSongs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration 
FROM StagSongs
WHERE song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO DimArtists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM StagSongs
WHERE artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO DimTime
(start_time, hour, day, week, month, year, weekday)
SELECT ts,
EXTRACT(hour from ts),
EXTRACT(day from ts),
EXTRACT(week from ts),
EXTRACT(month from ts),
EXTRACT(year from ts),
EXTRACT(dow from ts)
FROM StagEvents
""")

songplay_table_insert = ("""
INSERT INTO FactSongPlays 
(start_time, user_id, level, song_id,
artist_id, session_id, location, user_agent)
SELECT e.ts, e.userId, e.level, s.song_id,
       s.artist_id, e.sessionId, e.location, e.userAgent
FROM StagEvents e
LEFT JOIN StagSongs s
ON  e.artist = s.artist_name
AND e.song   = s.title
AND e.length = s.duration
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [user_table_insert, song_table_insert, artist_table_insert, time_table_insert, songplay_table_insert]