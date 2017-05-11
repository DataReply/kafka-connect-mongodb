package org.apache.kafka.connect.mongodb;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.mongodb.converter.StringStructConverter;
import org.apache.kafka.connect.mongodb.converter.StructConverter;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.bson.BsonTimestamp;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongodbSourceTask is a Task that reads mutations from a mongodb for storage in Kafka.
 *
 * @author Andrea Patelli
 */
public class MongodbSourceTask extends SourceTask {
    private final static Logger log = LoggerFactory.getLogger(MongodbSourceTask.class);

    private String uri;
    private String host;
    private Integer port;
    private String schemaName;
    private Integer batchSize;
    private String topicPrefix;
    private StructConverter structConverter;
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
    	if(map.containsKey(MongodbSourceConfig.PORT)){
	        try {
	            port = Integer.parseInt(map.get(MongodbSourceConfig.PORT));
	        } catch (Exception e) {
	            throw new ConnectException(MongodbSourceConfig.PORT + " config should be an Integer");
	        }
    	}
    	
        try {
            batchSize = Integer.parseInt(map.get(MongodbSourceConfig.BATCH_SIZE));
        } catch (Exception e) {
            throw new ConnectException(MongodbSourceConfig.BATCH_SIZE + " config should be an Integer");
        }

        schemaName = map.get(MongodbSourceConfig.SCHEMA_NAME);
        topicPrefix = map.get(MongodbSourceConfig.TOPIC_PREFIX);
        uri = map.get(MongodbSourceConfig.URI);
        host = map.get(MongodbSourceConfig.HOST);
        
        try{
            String structConverterClass = map.get(MongodbSourceConfig.CONVERTER_CLASS);
            if(structConverterClass == null || structConverterClass.isEmpty()){
            	structConverterClass = StringStructConverter.class.getName();
            }
            structConverter = (StructConverter) Class.forName(structConverterClass).newInstance();
        }
        catch(Exception e){
        	throw new ConnectException(MongodbSourceConfig.CONVERTER_CLASS + " config should be a class of type StructConverter");
        }
        
        databases = Arrays.asList(map.get(MongodbSourceConfig.DATABASES).split(","));

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
        
        if(uri != null){
        	reader = new MongodbReader(uri, databases, batchSize, offsets);
        }
        else{
        	reader = new MongodbReader(host, port, databases, batchSize, offsets);
        }
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
        List<SourceRecord> records = new ArrayList<>();
        while (!reader.isEmpty()) {
        	Document message = reader.pool();
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
    	if(reader != null){
    		reader.stop();
    	}
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
                .append("_")
                .append(timestamp.getInc())
                .toString();
    }

    /**
     * Creates a struct from a Mongodb message using configured {@link StructConverter}.
     *
     * @param message to parse
     * @return message formatted as a Struct
     */
    private Struct getStruct(Document message) {
    	final Schema schema = schemas.get(getDB(message).replaceAll("[\\s.]", "_"));
    	return structConverter.toStruct(message, schema);
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
