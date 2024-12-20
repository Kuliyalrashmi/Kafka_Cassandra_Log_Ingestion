<center>
  <image src = "https://github.com/user-attachments/assets/1ad64f32-7a5c-4442-9525-87bf6f1347ec" height = 400px >
</center>


# Kafka_Cassandra_Log_Ingestion

This project demonstrates a real-time logging pipeline that utilizes Kafka for streaming logs and Cassandra for data storage. Logs are generated by a custom producer, streamed into Kafka, and processed by a consumer that stores them in a Cassandra database.

---

## Key Features

1. **Kafka Producer:**
   - Generates synthetic logs with various log levels (INFO, WARNING, ERROR, DEBUG).
   - Serializes logs using Avro format.
   - Publishes logs to a Kafka topic.

2. **Kafka Consumer:**
   - Consumes logs from the Kafka topic.
   - Deserializes logs using the Avro schema.
   - Stores logs in a Cassandra database.

3. **Cassandra Integration:**
   - Ensures keyspace and table creation if they do not exist.
   - Inserts processed log records into the Cassandra database.

4. **Avro Schema:**
   - Defines the structure of log messages for serialization and deserialization.

---

## Project Structure

```
Kafka_Cassandra_Log_Ingestion/
├── producer.py              # Contains the Kafka Producer and log generation logic
├── consumer.py              # Contains the Kafka Consumer and Cassandra integration logic
├── avro_schema.avsc         # Avro schema for log serialization
└── README.md                # Project documentation
```

---

## Setup and Prerequisites

1. **Environment Setup:**
   - Install Kafka and ensure the broker is running on `localhost:9092`.
   - Install Cassandra and ensure it is running on `127.0.0.1`.

2. **Dependencies:**
   Install the required Python libraries using:
   ```bash
   pip install kafka-python cassandra-driver fastavro
   ```

3. **Avro Schema:**
   Place the Avro schema file (`avro_schema.avsc`) in the appropriate directory. The schema defines the structure of the logs:
   ```json
   {
       "name": "logSchema",
       "type": "record",
       "fields": [
           {"name": "timestamp", "type": "string"},
           {"name": "log_level", "type": "string"},
           {"name": "log_message", "type": "string"}
       ]
   }
   ```

---

## How to Run

### Step 1: Start the Kafka Producer
Run the producer to generate logs and publish them to Kafka:
```bash
python producer.py
```

### Step 2: Start the Kafka Consumer
Run the consumer to process logs and store them in Cassandra:
```bash
python consumer.py
```

---

## Cassandra Schema

The Cassandra table is defined as follows:
```cql
CREATE KEYSPACE IF NOT EXISTS logs_keyspace 
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS logs_keyspace.logs (
    timestamp TEXT,
    log_level TEXT,
    log_message TEXT,
    PRIMARY KEY (timestamp)
);
```

---

## Configuration

- **Kafka Configuration:**
  - Bootstrap Server: `localhost:9092`
  - Topic: `LogsRecord`

- **Cassandra Configuration:**
  - Host: `127.0.0.1`
  - Keyspace: `logs_keyspace`
  - Table: `logs`

---

## Future Improvements

- Implement fault-tolerance mechanisms in Kafka and Cassandra.
- Add monitoring for Kafka topics and consumer lags.
- Enhance log generation to simulate more realistic scenarios.
- Integrate visualization tools for log analysis.

---

## License

This project is licensed under the MIT License.
