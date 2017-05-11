package org.apache.kafka.connect.mongodb;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author Andrea Patelli
 */
public class MongodbSourceConnectorTest {
    private MongodbSourceConnector connector;
    private ConnectorContext context;


    @Before
    public void setupByHostAndPort() {
        connector = new MongodbSourceConnector();
        context = PowerMock.createMock(ConnectorContext.class);
        connector.initialize(context);
    }
    
    private Map<String, String> buildSourcePropertiesWithHostAndPort(){
    	final Map<String, String> sourceProperties = new HashMap<>();
        sourceProperties.put("host", "localhost");
        sourceProperties.put("port", Integer.toString(12345));
        sourceProperties.put("batch.size", Integer.toString(100));
        sourceProperties.put("schema.name", "schema");
        sourceProperties.put("topic.prefix", "prefix");
        sourceProperties.put("databases", "mydb.test1,mydb.test2,mydb.test3");
        return sourceProperties;
    }
    
    private Map<String, String> buildSourcePropertiesWithURI(){
    	final Map<String, String> sourceProperties = new HashMap<>();
        sourceProperties.put("uri", "mongodb://localhost:12345");
        sourceProperties.put("batch.size", Integer.toString(100));
        sourceProperties.put("schema.name", "schema");
        sourceProperties.put("topic.prefix", "prefix");
        sourceProperties.put("databases", "mydb.test1,mydb.test2,mydb.test3");
        return sourceProperties;
    }

    @Test
    public void testSourceTasks() {
        PowerMock.replayAll();
        connector.start(buildSourcePropertiesWithHostAndPort());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals("localhost", taskConfigs.get(0).get("host"));
        Assert.assertEquals("12345", taskConfigs.get(0).get("port"));
        Assert.assertEquals("100", taskConfigs.get(0).get("batch.size"));
        Assert.assertEquals("schema", taskConfigs.get(0).get("schema.name"));
        Assert.assertEquals("prefix", taskConfigs.get(0).get("topic.prefix"));
        Assert.assertEquals("mydb.test1,mydb.test2,mydb.test3", taskConfigs.get(0).get("databases"));
        PowerMock.verifyAll();
    }
    
    @Test
    public void testSourceTasksUri() {
        PowerMock.replayAll();
        connector.start(buildSourcePropertiesWithURI());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(1);
        Assert.assertEquals(1, taskConfigs.size());
        Assert.assertEquals("mongodb://localhost:12345", taskConfigs.get(0).get("uri"));
        Assert.assertEquals("100", taskConfigs.get(0).get("batch.size"));
        Assert.assertEquals("schema", taskConfigs.get(0).get("schema.name"));
        Assert.assertEquals("prefix", taskConfigs.get(0).get("topic.prefix"));
        Assert.assertEquals("mydb.test1,mydb.test2,mydb.test3", taskConfigs.get(0).get("databases"));
        PowerMock.verifyAll();
    }

    @Test
    public void testMultipleTasks() {
        PowerMock.replayAll();
        connector.start(buildSourcePropertiesWithHostAndPort());
        List<Map<String, String>> taskConfigs = connector.taskConfigs(2);
        Assert.assertEquals(2, taskConfigs.size());
        Assert.assertEquals("localhost", taskConfigs.get(0).get("host"));
        Assert.assertEquals("12345", taskConfigs.get(0).get("port"));
        Assert.assertEquals("100", taskConfigs.get(0).get("batch.size"));
        Assert.assertEquals("schema", taskConfigs.get(0).get("schema.name"));
        Assert.assertEquals("prefix", taskConfigs.get(0).get("topic.prefix"));
        Assert.assertEquals("mydb.test1,mydb.test2", taskConfigs.get(0).get("databases"));

        Assert.assertEquals("localhost", taskConfigs.get(1).get("host"));
        Assert.assertEquals("12345", taskConfigs.get(1).get("port"));
        Assert.assertEquals("100", taskConfigs.get(1).get("batch.size"));
        Assert.assertEquals("schema", taskConfigs.get(1).get("schema.name"));
        Assert.assertEquals("prefix", taskConfigs.get(1).get("topic.prefix"));
        Assert.assertEquals("mydb.test3", taskConfigs.get(1).get("databases"));
        PowerMock.verifyAll();
    }

    @Test
    public void testTaskClass() {
        PowerMock.replayAll();
        connector.start(buildSourcePropertiesWithHostAndPort());
        Assert.assertEquals(MongodbSourceTask.class, connector.taskClass());
        PowerMock.verifyAll();
    }
}
