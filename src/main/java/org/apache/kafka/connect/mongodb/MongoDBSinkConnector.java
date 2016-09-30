package org.apache.kafka.connect.mongodb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.mongodb.configuration.MongoDBSinkConfiguration;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.kafka.connect.utils.LogUtils;
import org.apache.kafka.connect.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * MongoDBSinkConnector implement the Connector interface to send Kafka
 * data to Mongodb
 *
 * @author Andrea Patelli
 * @author Niraj Patel
 */
public class MongoDBSinkConnector extends SinkConnector {

    private final static Logger log = LoggerFactory.getLogger(MongoDBSinkConnector.class);

    private Map<String, String> configuration;

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
     * @param configuration configuration settings
     */
    @Override
    public void start(Map<String, String> configuration) {
        MongoDBSinkConfiguration sinkConfiguration = new MongoDBSinkConfiguration(configuration);
        this.configuration = sinkConfiguration.originalsStrings();

        String collections = configuration.get(MongoDBSinkConfiguration.COLLECTIONS_CONFIG);
        String topics = configuration.get(MongoDBSinkConfiguration.TOPICS_CONFIG);
        if (collections.split(",").length != topics.split(",").length) {
            throw new ConnectException("The number of topics should be the same as the number of collections");
        }

        LogUtils.dumpConfiguration(configuration, log);
    }

    /**
     * Returns the task implementation for this Connector
     *
     * @return the task class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MongoDBSinkTask.class;
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
        List<Map<String, String>> configs = new ArrayList<>();
        List<String> coll = Arrays.asList(configuration.get(MongoDBSinkConfiguration.COLLECTIONS_CONFIG).split(","));
        int numGroups = Math.min(coll.size(), maxTasks);
        List<List<String>> dbsGrouped = ConnectorUtils.groupPartitions(coll, numGroups);
        List<String> topics = Arrays.asList(configuration.get(MongoDBSinkConfiguration.TOPICS_CONFIG).split(","));
        List<List<String>> topicsGrouped = ConnectorUtils.groupPartitions(topics, numGroups);

        IntStream.range(0, numGroups).forEach(i -> {
            Map<String, String> config = new HashMap<>();
            config.put(MongoDBSinkConfiguration.BULK_SIZE_CONFIG, configuration.get(MongoDBSinkConfiguration.BULK_SIZE_CONFIG));
            config.put(MongoDBSinkConfiguration.HOST_URLS_CONFIG, configuration.get(MongoDBSinkConfiguration.HOST_URLS_CONFIG));
            config.put(MongoDBSinkConfiguration.DATABASE_CONFIG, configuration.get(MongoDBSinkConfiguration.DATABASE_CONFIG));
            config.put(MongoDBSinkConfiguration.COLLECTIONS_CONFIG, StringUtils.join(dbsGrouped.get(i), ","));
            config.put(MongoDBSinkConfiguration.TOPICS_CONFIG, StringUtils.join(topicsGrouped.get(i), ","));
            configs.add(config);
        });
        return configs;
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {
        // not implemented
    }

    @Override
    public ConfigDef config() {
        return MongoDBSinkConfiguration.config;
    }

}
