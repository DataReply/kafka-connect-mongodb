package org.apache.kafka.connect.mongodb;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * MongodbSinkConnector implement the Connector interface to send Kafka
 * data to Mongodb
 *
 * @author Andrea Patelli
 */
public class MongodbSinkConnector extends SinkConnector {
    private final static Logger log = LoggerFactory.getLogger(MongodbSinkConnector.class);

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a string
     */
    @Override
    public String version() {
        return AppInfoParser.getVersion();
    }

    /**
     * Start this Connector. This method will only be called on a clean Connector, i.e. it has
     * either just been instantiated and initialized or {@link #stop()} has been invoked.
     *
     * @param map configuration settings
     */
    @Override
    public void start(Map<String, String> map) {

    }

    /**
     * Returns the task implementation for this Connector
     *
     * @return the task class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MongodbSinkTask.class;
    }

    /**
     * Returns a set of configurations for Tasks based on the current configuration,
     * producing at most maxTasks configurations.
     *
     * @param maxTasks maximum number of task to start
     * @return configurations for tasks
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        return null;
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {

    }
}
