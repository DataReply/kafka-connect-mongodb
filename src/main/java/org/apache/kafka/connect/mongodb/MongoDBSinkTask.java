package org.apache.kafka.connect.mongodb;

import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
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
import org.apache.kafka.connect.mongodb.configuration.MongoDBSinkConfiguration;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.kafka.connect.utils.SchemaUtils;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * MongoDBSinkTask is a Task that takes records loaded from Kafka and sends them to
 * mongodb.
 *
 * @author Andrea Patelli
 * @author Niraj Patel
 */
public class MongoDBSinkTask extends SinkTask {
    private final static Logger log = LoggerFactory.getLogger(MongoDBSinkTask.class);

    private Map<String, MongoCollection> mapping;

    private MongoDatabase db;

    private int bulkSize;
    private String collections;
    private String database;
    private String hostUrls;
    private String topics;

    @Override
    public String version() {
        return new MongoDBSinkConnector().version();
    }

    /**
     * Start the Task. Handles configuration parsing and one-time setup of the task.
     *
     * @param configuration initial configuration
     */
    @Override
    public void start(Map<String, String> configuration) {
        this.bulkSize = Integer.parseInt(configuration.get(MongoDBSinkConfiguration.BULK_SIZE_CONFIG));
        this.collections = configuration.get(MongoDBSinkConfiguration.COLLECTIONS_CONFIG);
        this.database = configuration.get(MongoDBSinkConfiguration.DATABASE_CONFIG);
        this.hostUrls = configuration.get(MongoDBSinkConfiguration.HOST_URLS_CONFIG);
        this.topics = configuration.get(MongoDBSinkConfiguration.TOPICS_CONFIG);

        List<String> collectionsList = Arrays.asList(collections.split(","));
        List<String> topicsList = Arrays.asList(topics.split(","));

        List<ServerAddress> addresses = Arrays.stream(hostUrls.split(",")).map(hostUrl -> {
            try {
                String[] hostAndPort = hostUrl.split(":");
                String host = hostAndPort[0];
                int port = Integer.parseInt(hostAndPort[1]);
                return new ServerAddress(host, port);
            } catch (ArrayIndexOutOfBoundsException aioobe) {
                throw new ConnectException("hosts must be in host:port format");
            } catch (NumberFormatException nfe) {
                throw new ConnectException("port in the hosts field must be an integer");
            }
        }).collect(Collectors.toList());

        MongoClient mongoClient = new MongoClient(addresses);
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

        IntStream.range(0, records.size()).forEach(i -> {
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
            bulks.keySet().forEach(key -> {
                try {
                    com.mongodb.bulk.BulkWriteResult result = mapping.get(key).bulkWrite(bulks.get(key));
                } catch (Exception e) {
                    log.error(e.getMessage());
                }
            });
        });
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void stop() {

    }
}
