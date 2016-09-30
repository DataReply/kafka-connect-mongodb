package org.apache.kafka.connect.mongodb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.mongodb.configuration.MongoDBSourceConfiguration;
import org.apache.kafka.connect.source.SourceConnector;
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
 * MongoDBSourceConnector implements the connector interface
 * to write on Kafka mutations received from a mongodb database
 *
 * @author Andrea Patelli
 * @author Niraj Patel
 */
public class MongoDBSourceConnector extends SourceConnector {

    private final static Logger log = LoggerFactory.getLogger(MongoDBSourceConnector.class);

    private Map<String, String> configuration;

    /**
     * Get the version of this connector.
     *
     * @return the version, formatted as a String
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
        MongoDBSourceConfiguration sourceConfiguration = new MongoDBSourceConfiguration(configuration);
        this.configuration = sourceConfiguration.originalsStrings();

        LogUtils.dumpConfiguration(configuration, log);
    }

    /**
     * Returns the Task implementation for this Connector.
     *
     * @return the Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MongoDBSourceTask.class;
    }

    /**
     * Returns a set of configuration for the Task based on the current configuration.
     *
     * @param maxTasks maximum number of configurations to generate
     * @return configurations for the Task
     */
    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        ArrayList<Map<String, String>> configs = new ArrayList<>();
        List<String> dbs = Arrays.asList(configuration.get(MongoDBSourceConfiguration.DATABASES_CONFIG).split(","));
        int numGroups = Math.min(dbs.size(), maxTasks);
        List<List<String>> dbsGrouped = ConnectorUtils.groupPartitions(dbs, numGroups);

        IntStream.range(0, numGroups).forEach(i -> {
            Map<String, String> config = new HashMap<>();
            config.put(MongoDBSourceConfiguration.HOST_URLS_CONFIG, configuration.get(MongoDBSourceConfiguration.HOST_URLS_CONFIG));
            config.put(MongoDBSourceConfiguration.SCHEMA_NAME_CONFIG, configuration.get(MongoDBSourceConfiguration.SCHEMA_NAME_CONFIG));
            config.put(MongoDBSourceConfiguration.BATCH_SIZE_CONFIG, configuration.get(MongoDBSourceConfiguration.BATCH_SIZE_CONFIG));
            config.put(MongoDBSourceConfiguration.TOPIC_PREFIX_CONFIG, configuration.get(MongoDBSourceConfiguration.TOPIC_PREFIX_CONFIG));
            config.put(MongoDBSourceConfiguration.DATABASES_CONFIG, StringUtils.join(dbsGrouped.get(i), ","));
            configs.add(config);
        });
        return configs;
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return MongoDBSourceConfiguration.config;
    }

}
