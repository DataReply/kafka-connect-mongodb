# Kafka Connect Mongodb
The connector is used to load data both from Kafka to Mongodb
and from Mongodb to Kafka.

# Building
You can build the connector with Maven using the standard lifecycle phases:
```
mvn clean
mvn package
```

# Source Connector
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

## Sample Configuration
```ini
name=mongodb-source-connector
connector.class=org.apache.kafka.connect.mongodb.MongodbSourceConnector
tasks.max=1
uri=mongodb://127.0.0.1:27017
batch.size=100
schema.name=mongodbschema
topic.prefix=optionalprefix
databases=mydb.test1,mydb.test2,mydb.test3
```

* **name**: name of the connector
* **connector.class**: class of the implementation of the connector
* **tasks.max**: maximum number of tasks to create
* **uri**: mongodb uri (required if host is not informed)
* **host**: mongodb host (required if uri is not informed)
* **port**: mongodb port (required if uri is not informed) 
* **batch.size**: maximum number of messages to write on Kafka at every poll() call
* **schema.name**: name to use for the schema, it will be formatted as ``{schema.name}_{database}_{collection}``
* **topic.prefix**: optional prefix to append to the topic names. The topic name is formatted as ``{topic.prefix}_{database}_{collection}``
* **converter.class**: converter class used to transform a mongodb oplog in a kafka message. We recommend use ``org.apache.kafka.connect.mongodb.converter.JsonStructConverter``, but due backward compatibility the default is ``org.apache.kafka.connect.mongodb.converter.StringStructConverter``. 
* **databases**: comma separated list of collections from which import data

# Sink Connector
When the connector is run as Sink, it retrieves messages from Kafka and writes them on mongodb collections. 
The structure of the written document is derived from the schema of the messages.

## Sample Configuration
```ini
name=mongodb-sink-connector
connector.class=org.apache.kafka.connect.mongodb.MongodbSinkConnector
tasks.max=1
uri=mongodb://127.0.0.1:27017
bulk.size=100
mongodb.database=databasetest
mongodb.collections=mydb_test1,mydb_test2,mydb_test3
converter.class=org.apache.kafka.connect.mongodb.converter.JsonStructConverter
topics=optionalprefix_mydb_test1,optionalprefix_mydb_test2,optionalprefix_mydb_test3
```

* **name**: name of the connector
* **connector.class**: class of the implementation of the connector
* **tasks.max**: maximum number of tasks to create
* **uri**: mongodb uri (required if host is not informed)
* **host**: mongodb host (required if uri is not informed)
* **port**: mongodb port (required if uri is not informed) 
* **bulk.size**: maximum number of documents to write on Mongodb at every put() call
* **mongodb.database**: database to use
* **mongodb.collections**: comma separated list of collections on which write the documents
* **topics**: comma separated list of topics to write on Mongodb

The number of collections and the number of topics should be the same.