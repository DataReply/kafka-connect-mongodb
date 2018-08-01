package org.apache.kafka.connect.mongodb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.kafka.connect.utils.LogUtils;
import org.apache.kafka.connect.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static org.apache.kafka.connect.mongodb.MongodbSourceConfig.PORT;
import static org.apache.kafka.connect.mongodb.MongodbSourceConfig.HOST;
import static org.apache.kafka.connect.mongodb.MongodbSourceConfig.URI;
import static org.apache.kafka.connect.mongodb.MongodbSourceConfig.SCHEMA_NAME;
import static org.apache.kafka.connect.mongodb.MongodbSourceConfig.BATCH_SIZE;
import static org.apache.kafka.connect.mongodb.MongodbSourceConfig.TOPIC_PREFIX;
import static org.apache.kafka.connect.mongodb.MongodbSourceConfig.CONVERTER_CLASS;
import static org.apache.kafka.connect.mongodb.MongodbSourceConfig.DATABASES;
import static org.apache.kafka.connect.mongodb.MongodbSourceConfig.CUSTOM_SCHEMA;

/**
 * MongodbSourceConnector implements the connector interface
 * to write on Kafka mutations received from a mongodb database
 *
 * @author Andrea Patelli
 */
public class MongodbSourceConnector extends SourceConnector {
    private final static Logger log = LoggerFactory.getLogger(MongodbSourceConnector.class);

    private String uri;
    private String port;
    private String host;
    private String schemaName;
    private String batchSize;
    private String topicPrefix;
    private String converterClass;
    private String databases;
    private String customSchema;

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

        uri = map.get(URI);
        if (uri == null || uri.isEmpty()){
            host = map.get(HOST);
            if (host == null || host.isEmpty()){
                throw new ConnectException("Missing " + HOST + "or " + URI +  " config");
            }

            port = map.get(PORT);
            if (port == null || port.isEmpty()){
                throw new ConnectException("Missing " + PORT + "or " + URI +  " config");
            }
        }
        schemaName = map.get(SCHEMA_NAME);
        if (schemaName == null || schemaName.isEmpty())
            throw new ConnectException("Missing " + SCHEMA_NAME + " config");

        batchSize = map.get(BATCH_SIZE);
        if (batchSize == null || batchSize.isEmpty())
            throw new ConnectException("Missing " + BATCH_SIZE + " config");

        databases = map.get(DATABASES);

        topicPrefix = map.get(TOPIC_PREFIX);

        converterClass = map.get(CONVERTER_CLASS);

        if(map.containsKey(CUSTOM_SCHEMA))
            customSchema = map.get(CUSTOM_SCHEMA);
        else
            customSchema = null;


        LogUtils.dumpConfiguration(map, log);
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
        for (int i = 0; i < numGroups; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(URI, uri);
            if(host!=null){
                config.put(HOST, host);
            }
            if(port!=null){
                config.put(PORT, port);
            }
            config.put(SCHEMA_NAME, schemaName);
            config.put(BATCH_SIZE, batchSize);
            config.put(TOPIC_PREFIX, topicPrefix);
            config.put(CONVERTER_CLASS, converterClass);
            config.put(DATABASES, StringUtils.join(dbsGrouped.get(i), ","));
            if(customSchema != null)
                config.put(CUSTOM_SCHEMA, customSchema);
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

    @Override
    public ConfigDef config () {
        return MongodbSourceConfig.config;
    }


}