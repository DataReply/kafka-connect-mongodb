package org.apache.kafka.connect.mongodb;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.connect.mongodb.converter.StringStructConverter;
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
    public static final String URI = "uri";
    private static final String URI_DOC = "uri of mongodb";
    public static final String BATCH_SIZE = "batch.size";
    private static final String BATCH_SIZE_DOC = "Count of documents in each polling";
    public static final String SCHEMA_NAME = "schema.name";
    private static final String SCHEMA_NAME_DOC = "Schema name";
    public static final String TOPIC_PREFIX = "topic.prefix";
    private static final String TOPIC_PREFIX_DOC = "Prefix of each topic, final topic will be prefix_db_collection";
    public static final String DATABASES = "databases";
    private static final String DATABASES_DOC = "Databases, join database and collection with dot, split different databases with comma";
    public static final String CONVERTER_CLASS = "converter.class";
    private static final String CONVERTER_CLASS_DOC = "Converter class used to transform a mongodb oplog in a kafka message";

    public static final String CUSTOM_SCHEMA = "custom.schema";
    private static final String CUSTOM_SCHEMA_DOC = "Flag that tells if a custom schema will be obtained from the data";

    public static ConfigDef config = new ConfigDef()
      .define(URI, Type.STRING, Importance.HIGH, URI_DOC)
      .define(HOST, Type.STRING, Importance.HIGH, HOST_DOC)
      .define(PORT, Type.INT, Importance.HIGH, PORT_DOC)
      .define(BATCH_SIZE, Type.INT, Importance.HIGH, BATCH_SIZE_DOC)
      .define(SCHEMA_NAME, Type.STRING, Importance.HIGH, SCHEMA_NAME_DOC)
      .define(TOPIC_PREFIX, Type.STRING, Importance.LOW, TOPIC_PREFIX_DOC)
      .define(CUSTOM_SCHEMA, Type.BOOLEAN, false, Importance.LOW, CUSTOM_SCHEMA_DOC)
      .define(CONVERTER_CLASS, Type.STRING, StringStructConverter.class.getName(), Importance.LOW, CONVERTER_CLASS_DOC)
      .define(DATABASES, Type.STRING, Importance.LOW, DATABASES_DOC);

    public MongodbSourceConfig(Map<String, String> props) {
        super(config, props);
    }
}
