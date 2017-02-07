package org.apache.kafka.connect.mongodb;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.utils.AppInfoParser;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkConnector;
import org.apache.kafka.connect.util.ConnectorUtils;
import org.apache.kafka.connect.utils.LogUtils;
import org.apache.kafka.connect.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import static org.apache.kafka.connect.mongodb.MongodbSinkConfig.HOST;
import static org.apache.kafka.connect.mongodb.MongodbSinkConfig.PORT;
import static org.apache.kafka.connect.mongodb.MongodbSinkConfig.BULK_SIZE;
import static org.apache.kafka.connect.mongodb.MongodbSinkConfig.TOPICS;
import static org.apache.kafka.connect.mongodb.MongodbSinkConfig.URI;
import static org.apache.kafka.connect.mongodb.MongodbSinkConfig.DATABASE;
import static org.apache.kafka.connect.mongodb.MongodbSinkConfig.COLLECTIONS;

/**
 * MongodbSinkConnector implement the Connector interface to send Kafka
 * data to Mongodb
 *
 * @author Andrea Patelli
 */
public class MongodbSinkConnector extends SinkConnector {
    private final static Logger log = LoggerFactory.getLogger(MongodbSinkConnector.class);

    private String uri;
    private String port;
    private String host;
    private String bulkSize;
    private String database;
    private String collections;
    private String topics;

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
        log.trace("Parsing configuration");

        uri = map.get(URI);
        if (uri == null || uri.isEmpty()){
            host = map.get(HOST);
            if (host == null || host.isEmpty()){
                throw new ConnectException("Missing " + HOST + " config");
            }
        	
	        port = map.get(PORT);
	        if (port == null || port.isEmpty()){
	            throw new ConnectException("Missing " + PORT + " config");
	        }
        }
        bulkSize = map.get(BULK_SIZE);
        if (bulkSize == null || bulkSize.isEmpty())
            throw new ConnectException("Missing " + BULK_SIZE + " config");

        database = map.get(DATABASE);
        collections = map.get(COLLECTIONS);
        topics = map.get(TOPICS);

        if (collections.split(",").length != topics.split(",").length) {
            throw new ConnectException("The number of topics should be the same as the number of collections");
        }

        LogUtils.dumpConfiguration(map, log);
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
        List<Map<String, String>> configs = new ArrayList<>();
        List<String> coll = Arrays.asList(collections.split(","));
        int numGroups = Math.min(coll.size(), maxTasks);
        List<List<String>> dbsGrouped = ConnectorUtils.groupPartitions(coll, numGroups);
        List<String> topics = Arrays.asList(this.topics.split(","));
        List<List<String>> topicsGrouped = ConnectorUtils.groupPartitions(topics, numGroups);

        for (int i = 0; i < numGroups; i++) {
            Map<String, String> config = new HashMap<>();
            config.put(URI, uri);
            if(host!=null){
                config.put(HOST, host);
            }
            if(port!=null){
            	config.put(PORT, port);
            }
            config.put(BULK_SIZE, bulkSize);
            config.put(DATABASE, database);
            config.put(COLLECTIONS, StringUtils.join(dbsGrouped.get(i), ","));
            config.put(TOPICS, StringUtils.join(topicsGrouped.get(i), ","));
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
        return null;
    }
}
