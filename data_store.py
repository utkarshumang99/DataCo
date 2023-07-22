# imput required : mysql_user, mysql_password, mysql_database
# replace the clickstream_aggregated_index according to the requirement


import mysql.connector
from kafka import KafkaConsumer
import json
from elasticsearch import Elasticsearch

# Configure Kafka settings
bootstrap_servers = 'localhost:9092'
topic = 'DataCo'  # Replace with the actual Kafka topic name for clickstream data

# Configure MySQL settings
mysql_host = 'localhost'
mysql_user = 'your_mysql_username'
mysql_password = 'your_mysql_password'
mysql_database = 'your_mysql_database'
mysql_table = 'clickstream_data'  # Table name for clickstream data

# Connect to MySQL
connection = mysql.connector.connect(
    host=mysql_host,
    user=mysql_user,
    password=mysql_password,
    database=mysql_database
)


# Function to create the table if it doesn't exist
def create_table_if_not_exists():
    cursor = connection.cursor()
    create_table_query = """
        CREATE TABLE IF NOT EXISTS {} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            user_id VARCHAR(50),
            timestamp INT,
            url VARCHAR(255),
            ip_address VARCHAR(100),
            user_agent VARCHAR(10000)
        )
    """.format(mysql_table)
    cursor.execute(create_table_query)
    connection.commit()


# Function to get the last processed timestamp - checkpoint
def get_last_processed_timestamp():
    cursor = connection.cursor()
    select_query = """
        SELECT MAX(timestamp) FROM {}
    """.format(mysql_table)
    cursor.execute(select_query)
    result = cursor.fetchone()
    return result[0] if result[0] else 0


# Function to store the last processed timestamp
def store_last_processed_timestamp(timestamp):
    cursor = connection.cursor()
    insert_query = """
        INSERT INTO checkpoint_table (timestamp)
        VALUES (%s)
    """
    cursor.execute(insert_query, (timestamp,))
    connection.commit()


# Function to process and aggregate click event data
def process_clickstream_data():
    last_processed_timestamp = get_last_processed_timestamp()

    cursor = connection.cursor()
    select_query = """
        SELECT url, country, COUNT(*) as num_clicks, COUNT(DISTINCT user_id) as num_unique_users, AVG(timestamp) as avg_time_spent
        FROM {} 
        WHERE timestamp > %s
        GROUP BY url, country
    """.format(mysql_table)
    cursor.execute(select_query, (last_processed_timestamp,))
    result = cursor.fetchall()

    if result:
        # Process the result and store aggregated data in MySQL
        # Create another table in MySQL to store the aggregated data
        create_aggregated_table_if_not_exists()
        for row in result:
            url, country, num_clicks, num_unique_users, avg_time_spent = row
            store_aggregated_data(url, country, num_clicks, num_unique_users, avg_time_spent)

        # Index the processed data in Elasticsearch
        index_data_in_elasticsearch()

        # Store the last processed timestamp
        last_processed_timestamp = result[-1][1]  # Assuming the timestamp is in the second position of the result
        store_last_processed_timestamp(last_processed_timestamp)

def create_aggregated_table_if_not_exists():
    cursor = connection.cursor()
    create_table_query = """
        CREATE TABLE IF NOT EXISTS clickstream_aggregated (
            id INT AUTO_INCREMENT PRIMARY KEY,
            url VARCHAR(255),
            country VARCHAR(100),
            num_clicks INT,
            num_unique_users INT,
            avg_time_spent FLOAT
        )
    """
    cursor.execute(create_table_query)
    connection.commit()

def store_aggregated_data(url, country, num_clicks, num_unique_users, avg_time_spent):
    cursor = connection.cursor()
    insert_query = """
        INSERT INTO clickstream_aggregated (url, country, num_clicks, num_unique_users, avg_time_spent)
        VALUES (%s, %s, %s, %s, %s)
    """
    aggregated_data = (url, country, num_clicks, num_unique_users, avg_time_spent)
    cursor.execute(insert_query, aggregated_data)
    connection.commit()

def index_data_in_elasticsearch():
    # Configure Elasticsearch settings
    es_host = 'localhost'
    es_port = 9200

    # Connect to Elasticsearch
    es = Elasticsearch([{'host': es_host, 'port': es_port}])

    cursor = connection.cursor()
    select_query = """
        SELECT url, country, num_clicks, num_unique_users, avg_time_spent
        FROM clickstream_aggregated
    """
    cursor.execute(select_query)
    result = cursor.fetchall()

    for row in result:
        url, country, num_clicks, num_unique_users, avg_time_spent = row
        index_aggregated_data(es, url, country, num_clicks, num_unique_users, avg_time_spent)


def index_aggregated_data(es, url, country, num_clicks, num_unique_users, avg_time_spent):
    doc = {
        'url': url,
        'country': country,
        'num_clicks': num_clicks,
        'num_unique_users': num_unique_users,
        'avg_time_spent': avg_time_spent
    }
    # Index the document in Elasticsearch
    # Replace 'clickstream_aggregated_index' with your desired index name
    es.index(index='clickstream_aggregated_index', body=doc)

# Create the table if it doesn't exist
create_table_if_not_exists()

# Kafka consumer to read from the Kafka topic
consumer = KafkaConsumer(topic,
                         bootstrap_servers=bootstrap_servers,
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))

# Main loop to continuously read and store click event data from Kafka
for message in consumer:
    click_event_data = message.value
    store_click_event(click_event_data)

    # Periodically process the stored data (e.g., every hour)
    if some_condition_to_trigger_processing():
        process_clickstream_data()
