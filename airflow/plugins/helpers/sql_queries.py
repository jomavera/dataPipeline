class SqlQueries:

    staging_events_table_create = ("""
        CREATE TABLE IF NOT EXISTS {} (
            artist VARCHAR,
            auth VARCHAR,
            firstName VARCHAR,
            gender VARCHAR,
            iteminSession INTEGER,
            lastName VARCHAR,
            length REAL,
            level VARCHAR,
            location VARCHAR,
            method VARCHAR,
            page VARCHAR,
            registration BIGINT,
            sessionId INTEGER,
            song VARCHAR,
            status INTEGER,
            ts TIMESTAMP,
            userAgent VARCHAR,
            userId INTEGER
        )
    """)

    staging_events_copy = (""" 
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        JSON '{}'
        REGION 'us-west-2'
        TIMEFORMAT 'epochmillisecs'
    """)

    staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS {} (
            num_songs INTEGER,
            artist_id VARCHAR,
            artist_latitude REAL,
            artist_longitude REAL,
            artist_location VARCHAR,
            artist_name VARCHAR,
            song_id VARCHAR,
            title VARCHAR,
            duration REAL,
            year INTEGER
        )
    """)

    staging_songs_copy = ("""
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{}'
    SECRET_ACCESS_KEY '{}'
    JSON '{}'
    REGION 'us-west-2'
    """)

    songplay_table_create = ("""
        CREATE TABLE IF NOT EXISTS songplays (
            songplay_id VARCHAR PRIMARY KEY,
            start_time TIMESTAMP,
            user_id INTEGER NOT NULL,
            level VARCHAR,
            song_id VARCHAR,
            artist_id VARCHAR,
            session_id INTEGER,
            location VARCHAR,
            user_agent VARCHAR
        )
    """)

    songplay_table_insert = ("""
        SELECT
                md5(events.sessionid || events.ts ) songplay_id,
                events.ts as start_time, 
                events.userid, 
                events.level, 
                songs.song_id, 
                songs.artist_id, 
                events.sessionid, 
                events.location, 
                events.useragent
                FROM (SELECT *
            FROM staging_events
            WHERE page='NextSong') events
            LEFT JOIN staging_songs songs
            ON events.song = songs.title
                AND events.artist = songs.artist_name
                AND events.length = songs.duration
    """)

    user_table_create = ("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY NOT NULL,
            first_name VARCHAR NOT NULL,
            last_name VARCHAR NOT NULL,
            gender VARCHAR,
            level VARCHAR NOT NULL
        )
    """)

    user_table_insert = ("""
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """)

    song_table_create = ("""
        CREATE TABLE IF NOT EXISTS songs (
            song_id VARCHAR PRIMARY KEY NOT NULL,
            title VARCHAR NOT NULL,
            artist_id VARCHAR NOT NULL,
            year INTEGER,
            duration REAL
        )
    """)

    song_table_insert = ("""
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """)

    artist_table_create = ("""
        CREATE TABLE IF NOT EXISTS artists (
            artist_id VARCHAR PRIMARY KEY NOT NULL,
            name VARCHAR NOT NULL,
            location VARCHAR,
            latitude REAL,
            longitude REAL
        )
    """)

    artist_table_insert = ("""
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """)

    time_table_create = ("""
        CREATE TABLE IF NOT EXISTS time (
            start_time TIMESTAMP PRIMARY KEY NOT NULL,
            hour INTEGER,
            day INTEGER,
            week INTEGER,
            month INTEGER,
            year INTEGER,
            weekday VARCHAR
        )
    """)

    time_table_insert = ("""
        SELECT start_time, extract(hour from start_time), extract(day from start_time), extract(week from start_time), 
               extract(month from start_time), extract(year from start_time), extract(dayofweek from start_time)
        FROM songplays
    """)

    check = ("""
            SELECT SUM(CASE WHEN {} IS NULL THEN 1 ELSE 0 END) AS NUM_NULLS
            FROM {};
    """)