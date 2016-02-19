package org.apache.kafka.connect.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.utils.SchemaUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * MongodbSinkTask is a Task that takes records loaded from Kafka and sends them to
 * mongodb.
 *
 * @author Andrea Patelli
 */
public class MongodbSinkTask extends SinkTask {
    private final static Logger log = LoggerFactory.getLogger(MongodbSinkTask.class);

    private Integer port;
    private String host;
    private Integer bulkSize;
    private String collections;
    private String database;
    private String topics;

    private Map<String, MongoCollection> mapping;
    private MongoDatabase db;

    @Override
    public String version() {
        return new MongodbSinkConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the task.
     *
     * @param map initial configuration
     */
    @Override
    public void start(Map<String, String> map) {
        try {
            port = Integer.parseInt(map.get(MongodbSinkConnector.PORT));
        } catch (Exception e) {
            throw new ConnectException("Setting " + MongodbSinkConnector.PORT + " should be an integer");
        }

        try {
            bulkSize = Integer.parseInt(map.get(MongodbSinkConnector.BULK_SIZE));
        } catch (Exception e) {
            throw new ConnectException("Setting " + MongodbSinkConnector.BULK_SIZE + " should be an integer");
        }

        database = map.get(MongodbSinkConnector.DATABASE);
        host = map.get(MongodbSinkConnector.HOST);
        collections = map.get(MongodbSinkConnector.COLLECTIONS);
        topics = map.get(MongodbSinkConnector.TOPICS);

        List<String> collectionsList = Arrays.asList(collections.split(","));
        List<String> topicsList = Arrays.asList(topics.split(","));

        MongoClient mongoClient = new MongoClient(host, port);
        db = mongoClient.getDatabase(database);

        mapping = new HashMap<>();

        for (int i = 0; i < topicsList.size(); i++) {
            String topic = topicsList.get(i);
            String collection = collectionsList.get(i);
            mapping.put(topic, db.getCollection(collection));
        }
    }

    /**
     * Put the records in the sink.
     *
     * @param collection the set of records to send.
     */
    @Override
    public void put(Collection<SinkRecord> collection) {
        List<SinkRecord> records = new ArrayList<>(collection);
        for (int i = 0; i < records.size(); i++) {
            Map<String, List<WriteModel<Document>>> bulks = new HashMap<>();

            for (int j = 0; j < bulkSize && i < records.size(); j++, i++) {
                SinkRecord record = records.get(i);
                Map<String, Object> jsonMap = SchemaUtils.toJsonMap((Struct) record.value());
                String topic = record.topic();

                if (bulks.get(topic) == null) {
                    bulks.put(topic, new ArrayList<WriteModel<Document>>());
                }

                Document newDocument = new Document(jsonMap)
                        .append("_id", record.kafkaOffset());

                log.trace("Adding to bulk: {}", newDocument.toString());
                bulks.get(topic).add(new UpdateOneModel<Document>(
                        Filters.eq("_id", record.kafkaOffset()),
                        new Document("$set", newDocument),
                        new UpdateOptions().upsert(true)));
            }
            i--;
            log.trace("Executing bulk");
            for (String key : bulks.keySet()) {
                try {
                    com.mongodb.bulk.BulkWriteResult result = mapping.get(key).bulkWrite(bulks.get(key));
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void stop() {

    }
}
