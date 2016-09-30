/*
 * Copyright HomeAway, Inc 2016-Present. All Rights Reserved.
 * No unauthorized use of this software.
 */

package org.apache.kafka.connect.mongodb.configuration;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.apache.kafka.connect.mongodb.MongoDBSourceConnector;

import java.util.Map;

/**
 * Configuration for {@link MongoDBSourceConnector}.
 *
 * @author Niraj Patel
 */
public class MongoDBSourceConfiguration extends AbstractConfig {

    private static final String MONGODB_GROUP = "MongoDB";
    private static final String CONNECTOR_GROUP = "Connector";

    /**
     * MongoDB Settings
     */
    public static final String HOST_URLS_CONFIG = "mongodb.host.urls";
    private static final String HOST_URLS_DOC = "The host URLs to use to connect to MongoDB.";
    private static final String HOST_URLS_DEFAULT = "http://127.0.0.1:27017";
    private static final String HOST_URLS_DISPLAY = "Host URLs (host:port comma seperated)";

    public static final String DATABASES_CONFIG = "mongodb.databases";
    private static final String DATABASES_DOC = "The databases to read from in MongoDB.";
    private static final String DATABASES_DISPLAY = "Database";

    /**
     * Connector Settings
     */
    public static final String BATCH_SIZE_CONFIG = "connector.batch.size";
    private static final String BATCH_SIZE_DOC = "The batch size to use when reading to MongoDB.";
    private static final int BATCH_SIZE_DEFAULT = 100;
    private static final String BATCH_SIZE_DISPLAY = "Connector batch size";

    public static final String SCHEMA_NAME_CONFIG = "connector.schema.name";
    private static final String SCHEMA_NAME_DOC = "Name to use for the schema.";
    private static final String SCHEMA_NAME_DISPLAY = "Schema name";

    public static final String TOPIC_PREFIX_CONFIG = "connector.topic.prefix";
    private static final String TOPIC_PREFIX_DOC = "Prefix to use for topic.";
    private static final String TOPIC_PREFIX_DEFAULT = "";
    private static final String TOPIC_PREFIX_DISPLAY = "Topic prefix";

    public MongoDBSourceConfiguration(Map<String, String> props) {
        super(config, props);
    }

    public static ConfigDef config = baseConfigDef();

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(HOST_URLS_CONFIG, Type.STRING, HOST_URLS_DEFAULT, Importance.HIGH, HOST_URLS_DOC, MONGODB_GROUP, 1, Width.LONG, HOST_URLS_DISPLAY)
                .define(DATABASES_CONFIG, Type.STRING, Importance.HIGH, DATABASES_DOC, MONGODB_GROUP, 2, Width.LONG, DATABASES_DISPLAY)
                .define(SCHEMA_NAME_CONFIG, Type.STRING, Importance.MEDIUM, SCHEMA_NAME_DOC, MONGODB_GROUP, 3, Width.LONG, SCHEMA_NAME_DISPLAY)
                .define(BATCH_SIZE_CONFIG, Type.INT, BATCH_SIZE_DEFAULT, Importance.MEDIUM, BATCH_SIZE_DOC, CONNECTOR_GROUP, 4, Width.LONG, BATCH_SIZE_DISPLAY)
                .define(TOPIC_PREFIX_CONFIG, Type.STRING, TOPIC_PREFIX_DEFAULT, Importance.LOW, TOPIC_PREFIX_DOC, CONNECTOR_GROUP, 5, Width.LONG, TOPIC_PREFIX_DISPLAY);
    }

}