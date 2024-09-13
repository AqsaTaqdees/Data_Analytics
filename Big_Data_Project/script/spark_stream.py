import logging
import uuid
import requests
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

logging.basicConfig(level=logging.INFO)

def create_keyspace(session):
    session.execute("""
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """)
    print("Keyspace created successfully!")

def create_table(session):
    session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.created_users (
            id UUID PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            gender TEXT,
            address TEXT,
            post_code TEXT,
            email TEXT,
            username TEXT,
            registered_date TEXT,
            phone TEXT,
            picture TEXT);
    """)
    print("Table created successfully!")

def insert_data(session, user_data):
    print("Inserting data...")

    try:
        session.execute("""
            INSERT INTO spark_streams.created_users(id, first_name, last_name, gender, address, 
                post_code, email, username, registered_date, phone, picture)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            uuid.uuid4(), user_data['name']['first'], user_data['name']['last'], user_data['gender'],
            f"{user_data['location']['street']['number']} {user_data['location']['street']['name']}",
            str(user_data['location']['postcode']), user_data['email'], user_data['login']['username'],
            user_data['registered']['date'], user_data['phone'], user_data['picture']['large']
        ))
        logging.info(f"Data inserted for {user_data['name']['first']} {user_data['name']['last']}")
    except Exception as e:
        logging.error(f'Could not insert data due to {e}')

def fetch_random_user():
    response = requests.get("https://randomuser.me/api/")
    if response.status_code == 200:
        return response.json()['results'][0]
    else:
        logging.error(f"Failed to fetch data from API, status code: {response.status_code}")
        return None

def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config('spark.jars.packages', "com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,"
                                           "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1") \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return s_conn
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception: {e}")
        return None

def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to: {e}")
        return None

def fetch_and_print_data(session):
    try:
        rows = session.execute("SELECT first_name, last_name FROM spark_streams.created_users;")
        for row in rows:
            print(row)
    except Exception as e:
        logging.error(f"Could not fetch data due to {e}")

def connect_to_kafka(spark_conn):
    # users_created is the topic name and earliest means that read data from the beginning
    spark_df = None
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'localhost:9092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()
        logging.info("Kafka dataframe created successfully")
    except Exception as e:
        logging.warning(f"Kafka dataframe could not be created because: {e}")
    return spark_df

def create_selection_df_from_kafka(spark_df):
    schema = StructType([
        StructField("id", StringType(), False),
        StructField("first_name", StringType(), False),
        StructField("last_name", StringType(), False),
        StructField("gender", StringType(), False),
        StructField("address", StringType(), False),
        StructField("post_code", StringType(), False),
        StructField("email", StringType(), False),
        StructField("username", StringType(), False),
        StructField("registered_date", StringType(), False),
        StructField("phone", StringType(), False),
        StructField("picture", StringType(), False)
    ])

    sel = spark_df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col('value'), schema).alias('data')).select("data.*")
    return sel

if __name__ == "__main__":
    # creating cassandra connection and keyspace/table setup
    session = create_cassandra_connection()
    if session is not None:
        create_keyspace(session)
        create_table(session)

        # fetching random user data and inserting into cassandra
        user_data = fetch_random_user()
        if user_data:
            insert_data(session, user_data)
            fetch_and_print_data(session)
        else:
            logging.error("No user data to insert")

    # creating spark connection
    spark_conn = create_spark_connection()
    if spark_conn is not None:
        # connect to kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        if spark_df is not None:
            selection_df = create_selection_df_from_kafka(spark_df)

            logging.info("Streaming is being started...")
            streaming_query = (selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                               .option('checkpointLocation', '/tmp/checkpoint')
                               .option('keyspace', 'spark_streams')
                               .option('table', 'created_users')
                               .start())
            streaming_query.awaitTermination()
