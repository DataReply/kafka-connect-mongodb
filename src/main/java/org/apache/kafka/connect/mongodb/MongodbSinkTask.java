package org.apache.kafka.connect.mongodb;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * MongodbSinkTask is a Task that takes records loaded from Kafka and sends them to
 * mongodb.
 *
 * @author Andrea Patelli
 */
public class MongodbSinkTask extends SinkTask {
    private final static Logger log = LoggerFactory.getLogger(MongodbSinkTask.class);

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

    }

    /**
     * Put the records in the sink.
     *
     * @param collection the set of records to send.
     */
    @Override
    public void put(Collection<SinkRecord> collection) {

    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {

    }

    @Override
    public void stop() {

    }
}
