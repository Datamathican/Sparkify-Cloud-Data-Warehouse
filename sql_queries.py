import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

#variables from config

ARN=config.get('IAM_ROLE','ARN')
LOG_DATA=config.get('S3','LOG_DATA')
SONG_DATA=config.get('S3','SONG_DATA')
LOG_JSONPATH=config.get('S3','LOG_JSONPATH')
REGION=config.get('S3','REGION')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events( artist VARCHAR, auth VARCHAR, firstName VARCHAR, gender VARCHAR,iteminSession INT, lastName VARCHAR, length NUMERIC, level VARCHAR, location VARCHAR, method VARCHAR, page VARCHAR, registration NUMERIC, sessionId INT, song VARCHAR, status INT, ts BIGINT, userAgent VARCHAR, userId INT);
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(num_songs INT, artist_id VARCHAR, artist_latitude NUMERIC, artist_longitude NUMERIC, artist_location VARCHAR, artist_name VARCHAR, song_id VARCHAR, title VARCHAR, duration NUMERIC, year INT);
""")

#FACT TABLE
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays(songplay_id INT IDENTITY(0,1) PRIMARY KEY SORTKEY, start_time TIMESTAMP NOT NULL, user_id INT NOT NULL , level VARCHAR, song_id VARCHAR, artist_id VARCHAR,session_id INT, location VARCHAR, user_agent VARCHAR);
""")

#DIMENSION TABLES

user_table_create = (""" CREATE TABLE IF NOT EXISTS users( user_id INT PRIMARY KEY SORTKEY, first_name VARCHAR,last_name VARCHAR, gender CHAR (1), level VARCHAR);
""")

song_table_create = (""" CREATE TABLE IF NOT EXISTS songs(song_id VARCHAR PRIMARY KEY SORTKEY, title VARCHAR, aritst_id VARCHAR NOT NULL, year INT, duration NUMERIC);
""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists( artist_id VARCHAR PRIMARY KEY SORTKEY, name VARCHAR, location VARCHAR, latitude NUMERIC, longitude NUMERIC);
""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY SORTKEY, hour INT, day INT, week INT, month INT, year INT, weekday INT);
""")

# STAGING TABLES

staging_events_copy = ("""COPY staging_events FROM '{}' IAM_ROLE '{}' REGION '{}' FORMAT AS JSON '{}' TIMEFORMAT AS'epochmillisecs';""").format(LOG_DATA,ARN,REGION, LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs FROM '{}' IAM_ROLE '{}' REGION '{}' FORMAT AS JSON 'auto';""").format(SONG_DATA,ARN,REGION)

# FINAL TABLES

#insert into fact table

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
SELECT TIMESTAMP 'epoch' + (e.ts/1000) * INTERVAL '1 second' AS start_time, e.userid, e.level, s.song_id, s.artist_id, e.session_id, e.location, e.user_agent FROM staging_events e JOIN staging_songs s ON (e.song=s.title and e.artist=s.artist_name) WHERE e.page='NextSong' AND e.userId IS NOT NULL;
""")
#insert into dimension tables

user_table_insert = ("""INSERT INTO users (user_id, first_name, last_name, gender, level) SELECT DISTINCT userID, firstName, lastName,gender, level FROM staging_events WHERE page ='NextSong' and userID IS NOT NULL;
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id,year, duration) SELECT DISTINCT song_id, title, artist_id,year, duration FROM staging_songs WHERE song_id IS NOT NULL AND artist_id IS NOT NULL; 
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude) SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude,artist_longitude FROM staging_songs WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday) SELECT DISTINCT start_time, EXTRACT (hour FROM start_time) ,EXTRACT (day FROM start_time) ,EXTRACT (week FROM start_time) ,EXTRACT (month FROM start_time),EXTRACT (year FROM start_time) ,EXTRACT (dayofweek FROM start_time)   FROM songplays WHERE page='NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
