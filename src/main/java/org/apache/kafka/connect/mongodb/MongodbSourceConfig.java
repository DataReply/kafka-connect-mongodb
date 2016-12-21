package org.apache.kafka.connect.mongodb;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Importance;

import java.util.Map;

/**
 * @author Xu Jingxin
 */
public class MongodbSourceConfig extends AbstractConfig {
    public static final String HOST = "host";
    private static final String HOST_DOC = "Host url of mongodb";
    public static final String PORT = "port";
    private static final String PORT_DOC = "Port of mongodb";
    public static final String BATCH_SIZE = "batch.size";
    private static final String BATCH_SIZE_DOC = "Count of documents in each polling";
    public static final String SCHEMA_NAME = "schema.name";
    private static final String SCHEMA_NAME_DOC = "Schema name";
    public static final String TOPIC_PREFIX = "topic.prefix";
    private static final String TOPIC_PREFIX_DOC = "Prefix of each topic, final topic will be prefix_db_collection";
    public static final String DATABASES = "databases";
    private static final String DATABASES_DOC = "Databases, join database and collection with dot, split different databases with comma";

    public static ConfigDef config = new ConfigDef()
            .define(HOST, Type.STRING, Importance.HIGH, HOST_DOC)
            .define(PORT, Type.INT, Importance.HIGH, PORT_DOC)
            .define(BATCH_SIZE, Type.INT, Importance.HIGH, BATCH_SIZE_DOC)
            .define(SCHEMA_NAME, Type.STRING, Importance.HIGH, SCHEMA_NAME_DOC)
            .define(TOPIC_PREFIX, Type.STRING, Importance.LOW, TOPIC_PREFIX_DOC)
            .define(DATABASES, Type.STRING, Importance.LOW, DATABASES_DOC);

    public MongodbSourceConfig(Map<String, String> props) {
        super(config, props);
    }
}
