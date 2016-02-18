package org.apache.kafka.connect.mongodb;

import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.kafka.connect.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * MongodbSourceConnector implements the connector interface
 * to write on Kafka mutations received from a mongodb database
 *
 * @author Andrea Patelli
 */
public class MongodbSourceConnector extends SourceConnector {
    private final static Logger log = LoggerFactory.getLogger(MongodbSourceConnector.class);

    public static final String PORT = "port";
    public static final String HOST = "host";
    public static final String SCHEMA_NAME = "schema.name";
    public static final String BATCH_SIZE = "batch.size";
    public static final String TOPIC_PREFIX = "topic.prefix";
    public static final String DATABASES = "databases";

    private String port;
    private String host;
    private String schemaName;
    private String batchSize;
    private String topicPrefix;
    private String databases;

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
     * @param map configuration settings
     */
    @Override
    public void start(Map<String, String> map) {
        log.trace("Parsing configuration");

        port = map.get(PORT);
        if (port == null || port.isEmpty())
            throw new ConnectException("Missing " + PORT + " config");

        schemaName = map.get(SCHEMA_NAME);
        if (schemaName == null || schemaName.isEmpty())
            throw new ConnectException("Missing " + SCHEMA_NAME + " config");

        batchSize = map.get(BATCH_SIZE);
        if (batchSize == null || batchSize.isEmpty())
            throw new ConnectException("Missing " + BATCH_SIZE + " config");

        host = map.get(HOST);
        if (host == null || host.isEmpty())
            throw new ConnectException("Missing " + HOST + " config");

        databases = map.get(DATABASES);

        topicPrefix = map.get(TOPIC_PREFIX);

        dumpConfiguration(map);
    }

    /**
     * Returns the Task implementation for this Connector.
     *
     * @return the Task implementation Class
     */
    @Override
    public Class<? extends Task> taskClass() {
        return MongodbSourceTask.class;
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
        List<String> dbs = Arrays.asList(databases.split(","));
        int numGroups = Math.min(dbs.size(), maxTasks);
        List<List<String>> dbsGrouped = ConnectorUtils.groupPartitions(dbs, numGroups);
        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(PORT, port);
            config.put(HOST, host);
            config.put(SCHEMA_NAME, schemaName);
            config.put(BATCH_SIZE, batchSize);
            config.put(TOPIC_PREFIX, topicPrefix);
            config.put(DATABASES, StringUtils.join(dbsGrouped, ","));
            configs.add(config);
        }
        return configs;
    }

    /**
     * Stop this connector.
     */
    @Override
    public void stop() {
    }

    private void dumpConfiguration(Map<String, String> map) {
        log.trace("Starting connector with configuration:");
        for (Map.Entry entry : map.entrySet()) {
            log.trace("{}: {}", entry.getKey(), entry.getValue());
        }
    }
}
