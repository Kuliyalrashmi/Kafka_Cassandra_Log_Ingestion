from kafka import KafkaConsumer,KafkaAdminClient
from kafka.errors import KafkaError
from fastavro.schema import load_schema
from fastavro import reader
import io
import logging
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

topic_name = "LogsRecord"
cassandra_host = ['127.0.0.1']
keyspace = 'logs_keyspace'
table = 'logs'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def AvroDeserializer(bytes_data):
    if bytes_data is None:
        return None
    try:
        bytes_reader = io.BytesIO(bytes_data)
        schema = load_schema('C:/Users/ujjwa/Desktop/DataEngineerin/Ingestion/Projects/RealTime Log Processing/logProcessing/AvroSchema/log_schema.avsc')
        return next(reader(bytes_reader, schema))
    except Exception as e:
        log.error(f"Error deserializing Avro data: {e}")
        return None

def InstantiateConsumer():
    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda value: AvroDeserializer(value),
        key_deserializer=lambda key: key.encode('utf-8'),
        group_id="LogConsumerGroup1",
        auto_offset_reset='earliest'
    )
    return consumer

def TopicExist(bootstrap_servers, topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers = bootstrap_servers)
    try:
        existing_topics = admin_client.list_topics()
        if topic_name not in existing_topics:
            log.error(f"Topic '{topic_name}' does not exist.")
            return False
        else:
            return True
    except KafkaError as e:
        log.error(f"Error while checking topic existence: {e}")
        return False
    finally:
        admin_client.close()

def ConnectCassandra():
    cluster = Cluster(cassandra_host)
    session = cluster.connect()
    session.set_keyspace(keyspace)
    return session

def InsertLogCassandra(session, timestamp, log_level, log_message):
    try:
        insert_query = f"INSERT INTO {keyspace}.{table} (timestamp, log_level, log_message) VALUES (%s, %s, %s)"
        session.execute(insert_query, (timestamp, log_level, log_message))
        log.info(f"Log inserted into Cassandra - Timestamp: {timestamp}, Level: {log_level}")
    except Exception as e:
        log.error(f"Error inserting log into Cassandra: {e}")

def CreateKeyspaceIfNotExist(cluster_host ,keyspace_name, replication_factor=1, strategy_class='SimpleStrategy'):
    session = None 
    cluster = None 
    try:
        cluster = Cluster(cluster_host)
        session = cluster.connect()

        keyspace_check_query = f"SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '{keyspace_name}'"
        rows = session.execute(keyspace_check_query)

        if not rows:
            create_keyspace_query = f"""
                CREATE KEYSPACE IF NOT EXISTS {keyspace_name}
                WITH REPLICATION  = {{
                    'class' : '{strategy_class}',
                    'replication_factor': '{replication_factor}' 
                }};
            """ 
            log.info(f"Creating keyspace {keyspace_name} with replication_factor = {replication_factor} and strategy_class = {strategy_class}...")
            session.execute(create_keyspace_query)
            log.info(f"Keyspace {keyspace_name} created successfully.")
        else:
            log.info(f"Keyspace {keyspace_name} already exists.")
    except Exception as e:
        log.error(f"Error creating keyspace {keyspace_name}: {e}")
    finally:
        if session:
            session.shutdown()
        if cluster:
            cluster.shutdown()


def main():
    if not TopicExist('localhost:9092', topic_name):
        log.error("Topic does not exist in Kafka.")
        return

    CreateKeyspaceIfNotExist(cassandra_host, keyspace)
    consumer = InstantiateConsumer()
    cassandra_session = ConnectCassandra()

    try:
        log.info(f"Consumer started, waiting for messages from topic '{topic_name}'...")
        records = consumer.poll(timeout_ms=1000)

        if not records:
            log.info("No messages received.")
            return

        for partitions, messages in records.items():
            for message in messages:
                log_data = message.value
                log_level = log_data.get('log_level', 'UNKNOWN')
                log_message = log_data.get('log_message', 'No message available')
                timestamp = log_data.get('timestamp', 'Unknown timestamp')

                log.info(f"Received Log - Timestamp: {timestamp}, Level: {log_level}, Message: {log_message}")
                InsertLogCassandra(cassandra_session, timestamp, log_level, log_message)

    except KeyboardInterrupt:
        log.info("Shutting down consumer...")
    except KafkaError as e:
        log.error(f"Kafka error: {e}")
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
