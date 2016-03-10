package org.apache.kafka.connect.mongodb;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * MongodbSourceTask is a Task that reads mutations from a mongodb for storage in Kafka.
 *
 * @author Andrea Patelli
 */
public class MongodbSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(MongodbSourceTask.class);

    private Integer port;
    private String host;
    private String schemaName;
    private Integer batchSize;
    private String topicPrefix;
    private List<String> databases;
    private static Map<String, Schema> schemas = null;

    private MongodbReader reader;


    Map<Map<String, String>, Map<String, Object>> offsets = new HashMap<>(0);


    @Override
    public String version() {
        return new MongodbSourceConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the Task.
     *
     * @param map initial configuration
     */
    @Override
    public void start(Map<String, String> map) {
        try {
            port = Integer.parseInt(map.get(MongodbSourceConnector.PORT));
        } catch (Exception e) {
            throw new ConnectException(MongodbSourceConnector.PORT + " config should be an Integer");
        }

        try {
            batchSize = Integer.parseInt(map.get(MongodbSourceConnector.BATCH_SIZE));
        } catch (Exception e) {
            throw new ConnectException(MongodbSourceConnector.BATCH_SIZE + " config should be an Integer");
        }

        schemaName = map.get(MongodbSourceConnector.SCHEMA_NAME);
        topicPrefix = map.get(MongodbSourceConnector.TOPIC_PREFIX);
        host = map.get(MongodbSourceConnector.HOST);
        databases = Arrays.asList(map.get(MongodbSourceConnector.DATABASES).split(","));

        log.trace("Creating schema");
        if (schemas == null) {
            schemas = new HashMap<>();
        }

        for (String db : databases) {
            db = db.replaceAll("[\\s.]", "_");
            if (schemas.get(db) == null)
                schemas.put(db,
                        SchemaBuilder
                                .struct()
                                .name(schemaName.concat("_").concat(db))
                                .field("timestamp", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("order", Schema.OPTIONAL_INT32_SCHEMA)
                                .field("operation", Schema.OPTIONAL_STRING_SCHEMA)
                                .field("database", Schema.OPTIONAL_STRING_SCHEMA)
                                .field("object", Schema.OPTIONAL_STRING_SCHEMA)
                                .build());
        }

        loadOffsets();
        reader = new MongodbReader(host, port, databases, offsets);
        reader.run();
    }

    /**
     * Poll this MongodbSourceTask for new records.
     *
     * @return a list of source records
     * @throws InterruptException
     */
    @Override
    public List<SourceRecord> poll() throws InterruptException {
        List<SourceRecord> records = new ArrayList<>(0);
        while (!reader.messages.isEmpty() && records.size() < batchSize) {
            Document message = reader.messages.poll();
            Struct messageStruct = getStruct(message);
            String topic = getTopic(message);
            String db = getDB(message);
            String timestamp = getTimestamp(message);
            records.add(new SourceRecord(Collections.singletonMap("mongodb", db), Collections.singletonMap(db, timestamp), topic, messageStruct.schema(), messageStruct));
            log.trace(message.toString());
        }


        return records;
    }

    /**
     * Signal this SourceTask to stop
     */
    @Override
    public void stop() {
    }

    /**
     * Retrieves a topic on which the message should be written.
     *
     * @param message from which retrieve the topic
     * @return parsed String representing the topic
     */
    private String getTopic(Document message) {
        String database = ((String) message.get("ns")).replaceAll("[\\s.]", "_");
        if (topicPrefix != null && !topicPrefix.isEmpty()) {
            return new StringBuilder()
                    .append(topicPrefix)
                    .append("_")
                    .append(database)
                    .toString();
        }
        return database;
    }

    /**
     * Retrieves the database from which the message has been read.
     *
     * @param message from which retrieve the database
     * @return the database name, as a String
     */
    private String getDB(Document message) {
        return (String) message.get("ns");
    }

    /**
     * Calculates the timestamp of the message.
     *
     * @param message from which retrieve the timestamp
     * @return BsonTimestamp formatted as a String (seconds+inc)
     */
    private String getTimestamp(Document message) {
        BsonTimestamp timestamp = (BsonTimestamp) message.get("ts");
        return new StringBuilder()
                .append(timestamp.getTime())
                .append(timestamp.getInc())
                .toString();
    }

    /**
     * Creates a struct from a Mongodb message.
     *
     * @param message to parse
     * @return message formatted as a Struct
     */
    private Struct getStruct(Document message) {
        Schema schema = schemas.get(getDB(message).replaceAll("[\\s.]", "_"));
        Struct messageStruct = new Struct(schema);
        BsonTimestamp bsonTimestamp = (BsonTimestamp) message.get("ts");
        Integer seconds = bsonTimestamp.getTime();
        Integer order = bsonTimestamp.getInc();
        messageStruct.put("timestamp", seconds);
        messageStruct.put("order", order);
        messageStruct.put("operation", message.get("op"));
        messageStruct.put("database", message.get("ns"));
        messageStruct.put("object", message.get("o").toString());
        return messageStruct;
    }

    /**
     * Loads the current saved offsets.
     */
    private void loadOffsets() {
        List<Map<String, String>> partitions = new ArrayList<>();
        for (String db : databases) {
            Map<String, String> partition = Collections.singletonMap("mongodb", db);
            partitions.add(partition);
        }
        offsets.putAll(context.offsetStorageReader().offsets(partitions));
    }
}
