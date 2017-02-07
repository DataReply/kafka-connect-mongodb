package org.apache.kafka.connect.mongodb;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import de.flapdoodle.embed.mongo.MongodExecutable;
import de.flapdoodle.embed.mongo.MongodProcess;
import de.flapdoodle.embed.mongo.MongodStarter;
import de.flapdoodle.embed.mongo.config.IMongodConfig;
import de.flapdoodle.embed.mongo.config.MongodConfigBuilder;
import de.flapdoodle.embed.mongo.config.Net;
import de.flapdoodle.embed.mongo.config.Storage;
import de.flapdoodle.embed.mongo.distribution.Version;
import de.flapdoodle.embed.process.runtime.Network;
import junit.framework.TestCase;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.bson.Document;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;
import org.powermock.api.easymock.PowerMock;

import java.io.File;
import java.util.*;

/**
 * @author Andre Ignacio
 */
public class MongodbSourceUriTaskTest extends TestCase {

    private static String REPLICATION_PATH = "/tmp/mongo";
    private MongodbSourceTask task;
    private SourceTaskContext context;
    private OffsetStorageReader offsetStorageReader;
    private Map<String, String> sourceProperties;

    private MongodExecutable mongodExecutable;
    private MongodProcess mongod;
    private MongodStarter mongodStarter;
    private IMongodConfig mongodConfig;
    private MongoClient mongoClient;

    private Map<String, Long> offsets;
    private long totalWrittenDocuments;
    private List<String> collections = new ArrayList<String>() {{
        add("test1");
        add("test2");
        add("test3");
    }};

    @Override
    public void setUp() {
        offsets = new HashMap<>();
        totalWrittenDocuments = 0;
        try {
            super.setUp();
            mongodStarter = MongodStarter.getDefaultInstance();
            mongodConfig = new MongodConfigBuilder()
                    .version(Version.Main.V3_2)
                    .replication(new Storage(REPLICATION_PATH, "rs0", 1024))
                    .net(new Net(12345, Network.localhostIsIPv6()))
                    .build();
            mongodExecutable = mongodStarter.prepare(mongodConfig);
            mongod = mongodExecutable.start();
            mongoClient = new MongoClient(new ServerAddress("localhost", 12345));
            MongoDatabase adminDatabase = mongoClient.getDatabase("admin");

            BasicDBObject replicaSetSetting = new BasicDBObject();
            replicaSetSetting.put("_id", "rs0");
            BasicDBList members = new BasicDBList();
            DBObject host = new BasicDBObject();
            host.put("_id", 0);
            host.put("host", "127.0.0.1:12345");
            members.add(host);
            replicaSetSetting.put("members", members);
            adminDatabase.runCommand(new BasicDBObject("isMaster", 1));
            adminDatabase.runCommand(new BasicDBObject("replSetInitiate", replicaSetSetting));
            MongoDatabase db = mongoClient.getDatabase("mydb");
            db.createCollection("test1");
            db.createCollection("test2");
            db.createCollection("test3");
        } catch (Exception e) {
//                Assert.assertTrue(false);
        }

        task = new MongodbSourceTask();

        offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);
        context = PowerMock.createMock(SourceTaskContext.class);
        task.initialize(context);

        sourceProperties = new HashMap<>();
        sourceProperties.put("uri", "mongodb://localhost:12345");
        sourceProperties.put("batch.size", Integer.toString(100));
        sourceProperties.put("schema.name", "schema");
        sourceProperties.put("topic.prefix", "prefix");
        sourceProperties.put("databases", "mydb.test1,mydb.test2,mydb.test3");

    }

    @Test
    public void testInsertWithNullOffsets() {
        try {
            expectOffsetLookupReturnNone();
            replay();

            task.start(sourceProperties);
            MongoDatabase db = mongoClient.getDatabase("mydb");

            Integer numberOfDocuments = new Random().nextInt(new Random().nextInt(100000));

            for (int i = 0; i < numberOfDocuments; i++) {
                Document newDocument = new Document()
                        .append(RandomStringUtils.random(new Random().nextInt(100), true, false), new Random().nextInt());
                db.getCollection(collections.get(new Random().nextInt(3))).insertOne(newDocument);
            }

            List<SourceRecord> records = new ArrayList<>();
            List<SourceRecord> pollRecords;
            do {
                pollRecords = task.poll();
                for(SourceRecord r : pollRecords) {
                    records.add(r);
                    offsets.putAll((Map<String, Long>)r.sourceOffset());
                }
            } while (!pollRecords.isEmpty());
            totalWrittenDocuments += records.size();
            Assert.assertEquals(totalWrittenDocuments, records.size());
        } catch (Exception e) {
            System.out.println("------------------------EXCEPTION-------------------------");
            e.printStackTrace();
            Assert.assertTrue(false);
            System.out.println("---------------------------END----------------------------");
        }
    }

    @Test
    public void testInsertWithOffsets() {
        try {
            expectOffsetLookupReturnOffset(collections);
            replay();

            task.start(sourceProperties);
            MongoDatabase db = mongoClient.getDatabase("mydb");

            Integer numberOfDocuments = new Random().nextInt(new Random().nextInt(100000));

            for (int i = 0; i < numberOfDocuments; i++) {
                Document newDocument = new Document()
                        .append(RandomStringUtils.random(new Random().nextInt(100), true, false), new Random().nextInt());
                db.getCollection(collections.get(new Random().nextInt(3))).insertOne(newDocument);
            }

            List<SourceRecord> records = new ArrayList<>();
            List<SourceRecord> pollRecords;
            do {
                pollRecords = task.poll();
                for(SourceRecord r : pollRecords) {
                    records.add(r);
                    offsets.putAll((Map<String, Long>)r.sourceOffset());
                }
            } while (!pollRecords.isEmpty());
            totalWrittenDocuments += records.size();
            Assert.assertEquals(totalWrittenDocuments, records.size());
        } catch (Exception e) {
            System.out.println("------------------------EXCEPTION-------------------------");
            e.printStackTrace();
            Assert.assertTrue(false);
            System.out.println("---------------------------END----------------------------");
        }
    }

    @Override
    public void tearDown() {
        try {
            super.tearDown();
            mongod.stop();
            mongodExecutable.stop();
            System.out.println("DELETING OPLOG");
            FileUtils.deleteDirectory(new File(REPLICATION_PATH));
        } catch (Exception e) {
        }
    }

    private void replay() {
        PowerMock.replayAll();
    }

    private void expectOffsetLookupReturnNone() {
        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offsets(EasyMock.anyObject(List.class))).andReturn(new HashMap<Map<String, String>, Map<String, Object>>(0));
    }

    private void expectOffsetLookupReturnOffset(List<String> databases) {
        Map<Map<String, String>, Map<String, Long>> offsetMap = new HashMap<>();
        for (int i = 0; i < databases.size(); i++) {
            offsetMap.put(
                    Collections.singletonMap("mongodb", databases.get(i)),
                    Collections.singletonMap(databases.get(i), offsets.get(databases.get(i)))
            );
        }
        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offsets(EasyMock.anyObject(List.class))).andReturn(offsetMap);
    }
}
