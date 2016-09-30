package org.apache.kafka.connect.mongodb;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.apache.kafka.connect.mongodb.configuration.MongoDBSourceConfiguration;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Andrea Patelli
 * @author Niraj Patel
 */
public class MongoDBSourceConnectorTest {

    private MongoDBSourceConnector connector;

    private ConnectorContext context;

    private Map<String, String> sourceProperties;

    @Before
    public void setup() {
        connector = new MongoDBSourceConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);

        sourceProperties = new HashMap<>();
        sourceProperties.put(MongoDBSourceConfiguration.HOST_URLS_CONFIG, "localhost:12345");
        sourceProperties.put(MongoDBSourceConfiguration.BATCH_SIZE_CONFIG, Integer.toString(100));
        sourceProperties.put(MongoDBSourceConfiguration.SCHEMA_NAME_CONFIG, "schema");
        sourceProperties.put(MongoDBSourceConfiguration.TOPIC_PREFIX_CONFIG, "prefix");
        sourceProperties.put(MongoDBSourceConfiguration.DATABASES_CONFIG, "mydb.test1,mydb.test2,mydb.test3");
    }

    @Test
    public void testSourceTasks() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals("localhost:12345", taskConfigs.get(0).get(MongoDBSourceConfiguration.HOST_URLS_CONFIG));
        Assert.assertEquals("100", taskConfigs.get(0).get(MongoDBSourceConfiguration.BATCH_SIZE_CONFIG));
        Assert.assertEquals("schema", taskConfigs.get(0).get(MongoDBSourceConfiguration.SCHEMA_NAME_CONFIG));
        Assert.assertEquals("prefix", taskConfigs.get(0).get(MongoDBSourceConfiguration.TOPIC_PREFIX_CONFIG));
        Assert.assertEquals("mydb.test1,mydb.test2,mydb.test3", taskConfigs.get(0).get(MongoDBSourceConfiguration.DATABASES_CONFIG));
        PowerMock.verifyAll();
    }

    @Test
    public void testMultipleTasks() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
        Assert.assertEquals(2, taskConfigs.size());
        Assert.assertEquals("localhost:12345", taskConfigs.get(0).get(MongoDBSourceConfiguration.HOST_URLS_CONFIG));
        Assert.assertEquals("100", taskConfigs.get(0).get(MongoDBSourceConfiguration.BATCH_SIZE_CONFIG));
        Assert.assertEquals("schema", taskConfigs.get(0).get(MongoDBSourceConfiguration.SCHEMA_NAME_CONFIG));
        Assert.assertEquals("prefix", taskConfigs.get(0).get(MongoDBSourceConfiguration.TOPIC_PREFIX_CONFIG));
        Assert.assertEquals("mydb.test1,mydb.test2", taskConfigs.get(0).get(MongoDBSourceConfiguration.DATABASES_CONFIG));

        Assert.assertEquals("localhost:12345", taskConfigs.get(1).get(MongoDBSourceConfiguration.HOST_URLS_CONFIG));
        Assert.assertEquals("100", taskConfigs.get(1).get(MongoDBSourceConfiguration.BATCH_SIZE_CONFIG));
        Assert.assertEquals("schema", taskConfigs.get(1).get(MongoDBSourceConfiguration.SCHEMA_NAME_CONFIG));
        Assert.assertEquals("prefix", taskConfigs.get(1).get(MongoDBSourceConfiguration.TOPIC_PREFIX_CONFIG));
        Assert.assertEquals("mydb.test3", taskConfigs.get(1).get(MongoDBSourceConfiguration.DATABASES_CONFIG));
        PowerMock.verifyAll();
    }

    @Test
    public void testTaskClass() {
        PowerMock.replayAll();
        connector.start(sourceProperties);
        Assert.assertEquals(MongoDBSourceTask.class, connector.taskClass());
        PowerMock.verifyAll();
    }
}
