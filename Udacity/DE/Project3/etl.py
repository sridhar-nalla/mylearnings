import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

print("Getting into etl.py")
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print("Processing ETL - load_staging_tables", query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print("Processing ETL - insert tables", query)
        cur.execute(query)
        conn.commit()


def main():
    import configparser
    print (" Getting into from etl - main method")
    config = configparser.ConfigParser()
    print (" Level-1 # etl.py")
    config.read('dwh.cfg')
    print (" Level-2 # etl.py")
    HOST = 'dwhcluster.cnpzjliajcrb.us-west-2.redshift.amazonaws.com' #config.get("CLUSTER","HOST")
    print (" Level-3 # etl.py")
#     config = configparser.ConfigParser()
#     config.read('dwh.cfg')

#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))


#######
    


    # CONFIG


    KEY                    = 'AKIAXIKZQKMMETJAGQCN' #config.get("AWS","KEY")
    SECRET                 = '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' #config.get("AWS","SECRET")
    DWH_DB                 = 'dwh' #config.get("CLUSTER","DB_NAME")
    DWH_DB_USER            = 'dwhuser' #config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD        = 'Passw0rd' #config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT               = 5439 #config.get("CLUSTER","DB_PORT")

    LOG_DATA = 's3://udacity-dend/log_data' #config.get("S3", "LOG_DATA")
    LOG_JSON_PATH = 's3://udacity-dend/log_json_path.json' #config.get("S3", "LOG_JSONPATH")
    SONG_DATA = 's3://udacity-dend/song_data' #config.get("S3", "SONG_DATA")
    ARN = 'arn:aws:iam::498940793624:role/apple' #config.get("IAM_ROLE", "ARN")

    DWH_ENDPOINT="dwhcluster.cnpzjliajcrb.us-west-2.redshift.amazonaws.com"
    DWH_ROLE_ARN="arn:aws:iam::498940793624:role/apple"
 
#     config = configparser.ConfigParser()
#     config.read('dwh.cfg')
    import os 
#     conn="postgresql://{}:{}@{}:{}/{}".format(DWH_DB_USER, DWH_DB_PASSWORD, DWH_ENDPOINT, DWH_PORT,DWH_DB)
#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect(f"host={HOST} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}")

    print(conn)
    # conn_string="postgresql://{}:{}@{}:{}/{}".format('awsuser', 'Uttara123', DWH_ENDPOINT, DWH_PORT,'dev')
#     print(conn)

################
#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
#     print(conn)
# #     %sql $conn_string
#     print (" Level-4 # Create_tables.py")
#     cur = conn.cursor()
    
    
#     conn.set_session(autocommit=True)
    cur = conn.cursor()
    print("etl.py entering to load_Staging_table program")
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()