class SqlQueries:
    songplay_table_insert = ("""
        INSERT INTO songsplay (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) SELECT
            TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
            e.userid,
            e.level,
            s.song_id,
            s.artist_id,
            e.sessionid,
            e.location,
            e.useragent
        FROM staging_events e
        LEFT JOIN staging_songs s ON
            e.song = s.title AND
            e.artist = s.artist_name AND
            ABS(e.length - s.duration) < 2
        WHERE
            e.page = 'NextSong'
    """)

    user_table_insert = ("""
    INSERT INTO users
        SELECT
            distinct e.userid,e.firstname,e.lastname,e.gender,e.level
        FROM 
            staging_events e
        WHERE
            e.page = 'NextSong'
    """)

    song_table_insert = ("""
    INSERT INTO songs
        SELECT
            distinct s.song_id, s.title, s.artist_id, s.year, s.duration
        FROM 
            staging_songs s
    """)

    artist_table_insert = ("""
    INSERT into artists
        SELECT
            distinct s.artist_id,s.artist_name,s.artist_location,
            s.artist_latitude,s.artist_longitude
        FROM 
            staging_songs s
    """)

    time_table_insert = ("""
    INSERT into times
        SELECT
            TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second'),
            extract(hour from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as hour,
            extract(day from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as day,
            extract(week from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as week,
            extract(month from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as month,
            extract(year from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as year,
            extract(weekday from TIMESTAMP 'epoch' + (e.ts/1000 * INTERVAL '1 second')) as weekday       
        FROM 
            staging_events e
        WHERE
            e.page = 'NextSong'
    """)