
Project Title# Sparkify Milion Song Dataset Analysis

Project Description#
    This project is about a music streaming startup called as Sparkify. Their data resides in S3, in a directory of JSON logs on user activity on the app and songs JSON metadata.
    As part of this project, build an ETL pipeline that extracts data from S3, stages them and transform data into a set of dim tables for finding insights into what songs user are listening to.


Details#
A) Data from storage [Datset]:
    i)   Song data     :  s3://udacity-dend/song_data
    ii)  Log data      :  s3://udacity-dend/log_data
    iii) Log data path :  s3://udacity-dend/log_json_path.json
B) Song Dataset
    Each file is in JSON format and contains metadata about a song and artist of that song.
    The file is partitioned by [first three letters of song's track id]
        song_data/A/B/C/TRAB123243.json
        song_data/A/A/C/TRA3434345.json
        
        {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null, "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud", "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff", "duration": 152.92036, "year": 0}
C) Log Dataset
    The log file is partitioned by [year and month]
    [log_data/2018/11/2018-11-12-events.json
    log_data/2018/11/2018-11-13-events.json]
    
    Use this link to understand the dataset, by clicking on the link below
    https://us-west-2.console.aws.amazon.com/s3/buckets/udacity-dend?region=us-west-2&tab=objects
        or use Athena to create a table to understand the data structure https://us-west-2.console.aws.amazon.com/athena/home?region=us-west-2#/data-sources/create-table
    
    Example, data in a log file, 2018-11-12-events.json
    artist aut       firstName gender itemInSession lastName length level location           method page registration sessionId song status ts             userAgent                      userId
    None   Logged In Celeste   F      0             Williams NaN    free  Klamath Falls, OR  GET    Home 1.541078e+12 438       None 200    1541990217796  "Mozilla/5.0 (Windows NT 6.1)"  53
D) Schema    
    1) Fact Table:        
            1. Songplays --> records event data associated with page "NextSong"
               songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
    2) Dimension Table:   
            2. users     --> users in the app
               user_id, first_name, last_name, gender, level
            
            3. songs     --> songs in the music database
               song_id, title, artist_id, year, duration
            
            4. artists   --> artists in music database
               artist_id, name, location, lattitude, longitude
            
            5. time      --> timestamp of records in songplays broken down into specific units
               start_time, hour, day, week, month, year, weekday

E) Project Template
    1. create_table.py is used to create fact and dimension tables for the star schema in Redshift.
    2. etl.py is used to load data from S3 into staging tables on Redshift and then process that data into analytics tables on Redshift.
    3. sql_queries.py is used to define SQL statements, which will be imported into the two other files above.
    

F) Project Steps: Following steps are used to create tables and building ETL pipeline
    a) Create Table Schemas
        1. Design schemas for fact and dimension tables
        2. Write a SQL CREATE statement for each of these tables in sql_queries.py
        3. Complete the logic in create_tables.py to connect to the database and create these tables
        4. Write SQL DROP statements to drop tables in the beginning of create_tables.py if the tables already exist. 
           This way, we can run create_tables.py to reset database and test ETL pipeline.
        5. Launch a redshift cluster and create an IAM role that has read access to S3.
        6. Add redshift database and IAM role info to dwh.cfg.
        7. Test by running create_tables.py and checking the table schemas in redshift database. Use Query Editor in the AWS Redshift console for this.
    b) Build ETL Pipeline
        1. Implement the logic in etl.py to load data from S3 to staging tables on Redshift.
        2. Implement the logic in etl.py to load data from staging tables to analytics tables on Redshift.
        3. Test by running etl.py after running create_tables.py and running the analytic queries on Redshift database to compare results with the expected results.
        4. Delete redshift cluster when finished.
    c) Document Process: README.md file.
        1. Discuss the purpose of this database in context of the startup, Sparkify, and their analytical goals.
        2. State and justify your database schema design and ETL pipeline.
        3. [Optional] Provide example queries and results for song play analysis.
        
