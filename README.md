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