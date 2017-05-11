package org.apache.kafka.connect.mongodb;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

/**
 * @author Xu Jingxin
 */
public class MongodbSinkConfig extends AbstractConfig {
    public static final String HOST = "host";
    private static final String HOST_DOC = "Host url of mongodb";
    public static final String PORT = "port";
    private static final String PORT_DOC = "Port of mongodb";
    public static final String URI = "uri";
    private static final String URI_DOC = "uri of mongodb";
    public static final String BULK_SIZE = "bulk.size";
    private static final String BULK_SIZE_DOC = "Count of documents in each polling";
    public static final String TOPICS = "topics";
    private static final String TOPIC_PREFIX_DOC = "Topics";
    public static final String DATABASE = "mongodb.database";
    private static final String DATABASE_DOC = "Database of mongodb";
    public static final String COLLECTIONS = "mongodb.collections";
    private static final String COLLECTIONS_DOC = "Collections of mongodb";

    public static ConfigDef config = new ConfigDef()
    		.define(URI, Type.STRING, Importance.HIGH, URI_DOC)
            .define(HOST, Type.STRING, Importance.HIGH, HOST_DOC)
            .define(PORT, Type.INT, Importance.HIGH, PORT_DOC)
            .define(BULK_SIZE, Type.INT, Importance.HIGH, BULK_SIZE_DOC)
            .define(TOPICS, Type.STRING, Importance.LOW, TOPIC_PREFIX_DOC)
            .define(DATABASE, Type.STRING, Importance.LOW, DATABASE_DOC)
            .define(COLLECTIONS, Type.STRING, Importance.LOW, COLLECTIONS_DOC);

    public MongodbSinkConfig(Map<String, String> props) {
        super(config, props);
    }
}
