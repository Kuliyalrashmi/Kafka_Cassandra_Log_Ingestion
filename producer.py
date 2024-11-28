import time
import random
from kafka import KafkaProducer , KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
from fastavro.schema import load_schema
from fastavro import writer
import json 
import io


class CustomLogGenerator:
    def __init__(self):
        self.log_levels = ['INFO', 'WARNING', 'ERROR', 'DEBUG']
        self.log_messages = {
            'INFO': ["User logged in.", "Application started.", "New user registered."],
            'WARNING': ["Disk space low.", "High memory usage detected.", "CPU utilization spike."],
            'ERROR': ["Database connection failed.", "Payment processing error.", "Failed to load configuration."],
            'DEBUG': ["Debugging data flow.", "Analyzing module interaction.", "Debug mode activated."]
        }

    def to_dict(self , timestamp , log_level , log_message ):
        return{
            "timestamp" : timestamp,
            "log_level" : log_level,
            "log_message" : log_message 
        } 

    def generate_log(self):
        log_level = random.choice(self.log_levels)
        log_message = random.choice(self.log_messages[log_level])
        timestamp = time.strftime('%Y-%m-%d %H:%M:%S')
        log = self.to_dict(timestamp , log_level , log_message)
        yield log
        time.sleep(random.uniform(0.5, 2)) 


def AvroSerializer(data):
    bytes_writes = io.BytesIO()
    schema = load_schema('C:/Users/ujjwa/Desktop/DataEngineerin/Ingestion/Projects/RealTime Log Processing/logProcessing/AvroSchema/log_schema.avsc')
    writer(bytes_writes , schema , [data])
    return bytes_writes.getvalue()

def success(metadata):
    print(f"data stored in topic {metadata.topic} || partitin {metadata.partition} || offset{metadata.offset}" )

def error(excep):
    print(f"Exception While Writting Data : {excep}")

def CheckTopicExists(bootstrap_servers , topic_name):
    admin_client = KafkaAdminClient(bootstrap_servers = bootstrap_servers)

    try:
        existing_topics = admin_client.list_topics()

        if topic_name in existing_topics:
            return True
        else :
            return False
    except Exception as e :
        print(f"Error checking topic existence: {e}")
        return False
    finally:
        admin_client.close()

def CreateKafkaTopic(bootstrap_servers , topic_name, num_parititons , replication_factor):
    admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)

    if(CheckTopicExists(bootstrap_servers, topic_name)):
        print("Topic Already Exists..")
        return

    new_topic  = NewTopic(
        name = topic_name,
        replication_factor= replication_factor,
        num_parititons = num_parititons
    )

    try:
        admin_client.create_topics(new_topics=[new_topic], validate_only=False)
        print(f"Topic '{topic_name}' created successfully with {num_partitions} partitions and a replication factor of {replication_factor}.")
    except Exception as e:
        print(f"Failed to create topic '{topic_name}': {e}")
    finally:
        admin_client.close()

def ProducerInstantiate():
    producer = KafkaProducer(
        bootstrap_servers = ['localhost:9092'],
        batch_size = 32000,
        value_serializer = lambda value : AvroSerializer(value),
        key_serializer = lambda key : key.encode('utf-8')
    )

    return producer

def main():
    log_generator  = CustomLogGenerator()
    producer = ProducerInstantiate()
    CreateKafkaTopic("localhost:9092" , "LogsRecord" , 3, 1)

    try :
        while True :  
            for log in log_generator.generate_log():
                producer.send(topic = "LogsRecord" , value = log , key  = log["log_level"] ).add_callback(success).add_errback(error)
    except KeyboardInterrupt:
        print("Shutting Down Log Generator .. ")
        print("Shutting Down Producer !!")
    except KafkaError as e:
        print(f"Kafka Error : {e} ")
    finally:
        producer.flush()
        producer.close() 

if __name__ == "__main__":
    main()