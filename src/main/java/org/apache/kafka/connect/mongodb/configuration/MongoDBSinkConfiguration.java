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
import org.apache.kafka.connect.mongodb.MongoDBSinkConnector;

import java.util.Map;

/**
 * Configuration for {@link MongoDBSinkConnector}.
 *
 * @author Niraj Patel
 */
public class MongoDBSinkConfiguration extends AbstractConfig {

    private static final String MONGODB_GROUP = "MongoDB";
    private static final String CONNECTOR_GROUP = "Connector";

    /**
     * MongoDB Settings
     */
    public static final String TOPICS_CONFIG = "topics";

    public static final String HOST_URLS_CONFIG = "mongodb.host.urls";
    private static final String HOST_URLS_DOC = "The host URLs to use to connect to MongoDB.";
    private static final String HOST_URLS_DEFAULT = "http://127.0.0.1:27017";
    private static final String HOST_URLS_DISPLAY = "Host URLs (host:port comma seperated)";

    public static final String DATABASE_CONFIG = "mongodb.database";
    private static final String DATABASE_DOC = "The database to write to in MongoDB.";
    private static final String DATABASE_DISPLAY = "Database";

    public static final String COLLECTIONS_CONFIG = "mongodb.database.collections";
    private static final String COLLECTIONS_DOC = "The collection to write to in MongoDB database.";
    private static final String COLLECTIONS_DISPLAY = "Collections (comma seperated)";

    /**
     * Connector settings
     */
    public static final String BULK_SIZE_CONFIG = "connector.bulk.size";
    private static final String BULK_SIZE_DOC = "The bulk size to use when writing to MongoDB.";
    private static final int BULK_SIZE_DEFAULT = 100;
    private static final String BULK_SIZE_DISPLAY = "Connector bulk size";

    public MongoDBSinkConfiguration(Map<String, String> props) {
        super(config, props);
    }

    public static ConfigDef config = baseConfigDef();

    public static ConfigDef baseConfigDef() {
        return new ConfigDef()
                .define(HOST_URLS_CONFIG, Type.STRING, HOST_URLS_DEFAULT, Importance.HIGH, HOST_URLS_DOC, MONGODB_GROUP, 1, Width.LONG, HOST_URLS_DISPLAY)
                .define(DATABASE_CONFIG, Type.STRING, Importance.HIGH, DATABASE_DOC, MONGODB_GROUP, 2, Width.LONG, DATABASE_DISPLAY)
                .define(COLLECTIONS_CONFIG, Type.STRING, Importance.HIGH, COLLECTIONS_DOC, MONGODB_GROUP, 3, Width.LONG, COLLECTIONS_DISPLAY)
                .define(BULK_SIZE_CONFIG, Type.INT, BULK_SIZE_DEFAULT, Importance.MEDIUM, BULK_SIZE_DOC, CONNECTOR_GROUP, 4, Width.LONG, BULK_SIZE_DISPLAY);
    }

}