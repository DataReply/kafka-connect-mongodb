# Kafka Connect Mongodb
The connector is used to load data both from Kafka to Mongodb
and from Mongodb to Kafka.

# Building
You can build the connector with Maven using the standard lifecycle phases:
```
mvn clean
mvn package
```

# Sample Configuration
## Source Connector
When the connector is run as a Source Connector, it reads data from [Mongodb oplog](https://docs.mongodb.org/manual/core/replica-set-oplog/)
and publishes it on Kafka.
3 different types of messages are read from the oplog:
* Insert
* Update
* Delete
For every message, a SourceRecord is created, having the following schema:
```json
{
  "type": "record",
  "name": "schemaname",
  "fields": [
    {
      "name": "timestamp",
      "type": [
        "null",
        "int"
      ]
    },
    {
      "name": "order",
      "type": [
        "null",
        "int"
      ]
    },
    {
      "name": "operation",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "database",
      "type": [
        "null",
        "string"
      ]
    },
    {
      "name": "object",
      "type": [
        "null",
        "string"
      ]
    }
  ],
  "connect.name": "stillmongotesting"
}
```
* **timestamp**: timestamp in seconds when the event happened
* **order**: order of the event between events with the same timestamp
* **operation**: type of operation the message represent. i: insert, u: update, d: delete
* **database**: database in which the operation took place
* **object**: inserted/updated/deleted object

```ini
name=mongodb-source-connector
connector.class=org.apache.kafka.connect.mongodb.MongodbSourceConnector
tasks.max=1
host=127.0.0.1
port=27017
batch.size=100
schema.name=mongodbschema
topic.prefix=optionalprefix
databases=mydb.test1,mydb.test2,mydb.test3
```

## Sink Connector
```ini
name=mongodb-sink-connector
connector.class=org.apache.kafka.connect.mongodb.MongodbSinkConnector
tasks.max=1
host=127.0.0.1
port=27017
bulk.size=100
mongodb.database=databasetest
mongodb.collections=mydb_test1,mydb_test2,mydb_test3
```