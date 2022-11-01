import configparser
import psycopg2
from sql_queries import drop_table_queries
from sql_queries import create_table_queries

def drop_tables(cur, conn):
    for query in drop_table_queries:
        print("Processing - Delete table ", query)
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        print("Processing - Create table  ", query)
        cur.execute(query)
        conn.commit()


def main():
    print (" Getting into from Create_tables.py")
    config = configparser.ConfigParser()
#     print (" Level-1 # Create_tables.py")
    config.read('dwh.cfg')
#     print (" Level-2 # Create_tables.py")
    HOST = 'dwhcluster.cnpzjliajcrb.us-west-2.redshift.amazonaws.com' #config.get("CLUSTER","HOST")
#     print (" Level-3 # Create_tables.py")
    # [AWS]
# KEY='AKIAXIKZQKMMETJAGQCN'
# SECRET='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'
    KEY                    = 'AKIAXIKZQKMMETJAGQCN' #config.get("AWS","KEY")
    SECRET                 = '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' #config.get("AWS","SECRET")
    DWH_DB                 = 'dwh' #config.get("CLUSTER","DB_NAME")
    DWH_DB_USER            = 'dwhuser' #config.get("CLUSTER","DB_USER")
    DWH_DB_PASSWORD        = 'Passw0rd' #config.get("CLUSTER","DB_PASSWORD")
    DWH_PORT               = 5439 #config.get("CLUSTER","DB_PORT")

    DWH_IAM_ROLE_NAME      = 'arn:aws:iam::498940793624:role/apple' #config.get("IAM_ROLE", "ARN")
#     conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    print("Connection at SQL queries table")
    conn = psycopg2.connect(f"host={HOST} dbname={DWH_DB} user={DWH_DB_USER} password={DWH_DB_PASSWORD} port={DWH_PORT}")
    print(conn)
#     %sql $conn_string
#     print (" Level-4 # Create_tables.py")
    cur = conn.cursor()
    print (" Level-5 # Create_tables.py")
    drop_tables(cur, conn)
    print (" Level-6 # Create_tables.py")
    create_tables(cur, conn)

    conn.close()
    print (" Level-7 # Create_tables.py")


if __name__ == "__main__":
    main()
    
#     [CLUSTER]
# HOST=dwhcluster.cnpzjliajcrb.us-west-2.redshift.amazonaws.com
# DB_NAME=dwh
# DB_USER=dwhuser
# DB_PASSWORD=Passw0rd
# DB_PORT=5439

# [IAM_ROLE]
# ARN='arn:aws:iam::498940793624:role/apple'

# [S3]
# LOG_DATA='s3://udacity-dend/log_data'
# LOG_JSONPATH='s3://udacity-dend/log_json_path.json'
# SONG_DATA='s3://udacity-dend/song_data'

# SELECT * FROM information_schema.sql_languages


# [AWS]
# KEY='AKIAXIKZQKMMETJAGQCN' #Latest
# SECRET='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' #Latest

# [DWH] 
# DWH_CLUSTER_TYPE=multi-node
# DWH_NUM_NODES=4
# DWH_NODE_TYPE=dc2.large

# DWH_IAM_ROLE_NAME=apple
# DWH_CLUSTER_IDENTIFIER=dwhCluster
# DWH_DB=dwh
# DWH_DB_USER=dwhuser
# DWH_DB_PASSWORD=Passw0rd
# DWH_PORT=5439
