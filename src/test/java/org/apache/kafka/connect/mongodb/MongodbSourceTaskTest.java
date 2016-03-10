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
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.bson.Document;
import org.easymock.EasyMock;
import org.junit.*;
import org.powermock.api.easymock.PowerMock;

import java.io.File;
import java.util.*;

/**
 * @author Andrea Patelli
 */
public class MongodbSourceTaskTest {

    private static String REPLICATION_PATH = "/home/vagrant/mongo";
    private MongodbSourceTask task;
    private SourceTaskContext context;
    private OffsetStorageReader offsetStorageReader;
    private Map<String, String> sourceProperties;

    private MongodExecutable mongodExecutable;
    private MongodProcess mongod;
    private MongodStarter mongodStarter;
    private IMongodConfig mongodConfig;

    @Before
    public void setup() {
        if(mongod == null) {
            try {
                mongodStarter = MongodStarter.getDefaultInstance();
                mongodConfig = new MongodConfigBuilder()
                        .version(Version.Main.V3_2)
                        .replication(new Storage(REPLICATION_PATH, "rs0", 1024))
                        .net(new Net(12345, Network.localhostIsIPv6()))
                        .build();
                mongodExecutable = mongodStarter.prepare(mongodConfig);
                mongod = mongodExecutable.start();
                MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 12345));
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
                mongoClient.close();
            } catch (Exception e) {
                Assert.assertTrue(false);
            }
        }

        task = new MongodbSourceTask();
        offsetStorageReader = PowerMock.createMock(OffsetStorageReader.class);
        context = PowerMock.createMock(SourceTaskContext.class);
        task.initialize(context);

        sourceProperties = new HashMap<>();
        sourceProperties.put("host", "localhost");
        sourceProperties.put("port", Integer.toString(12345));
        sourceProperties.put("batch.size", Integer.toString(100));
        sourceProperties.put("schema.name", "schema");
        sourceProperties.put("topic.prefix", "prefix");
        sourceProperties.put("databases", "mydb.test1,mydb.test2,mydb.test3");
    }

    @Test
    public void testNormalLifecycle() {
        try {
            List<String> collections = new ArrayList<String>() {{
                add("test1");
                add("test2");
                add("test3");
            }};
            expectOffsetLookupReturnOffset(collections);
            replay();

            task.start(sourceProperties);
            System.out.println("CREATING MONGO CLIENT");
            MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 12345));

            MongoDatabase db = mongoClient.getDatabase("mydb");
            db.createCollection("test1");
            db.createCollection("test2");
            db.createCollection("test3");

            Integer numberOfDocuments = new Random().nextInt(new Random().nextInt(100000));

            for(int i = 0; i < numberOfDocuments; i++) {
                Document newDocument = new Document()
                        .append(RandomStringUtils.random(new Random().nextInt(100), true, false), new Random().nextInt());
                db.getCollection(collections.get(new Random().nextInt(3))).insertOne(newDocument);
            }

            List<SourceRecord> records = new ArrayList<>();
            List<SourceRecord> pollRecords;
            do {
                pollRecords = task.poll();
                records.addAll(pollRecords);
            } while(!pollRecords.isEmpty());
            System.out.println("SIZE " + records.size());
            Assert.assertEquals((long) numberOfDocuments, records.size());

            // drop collection
            System.out.println("DELETING COLLECTIONS");
            db.getCollection("test1").drop();
            db.getCollection("test2").drop();
            db.getCollection("test3").drop();

            mongoClient.close();
        } catch(Exception e) {
            System.out.println("------------------------EXCEPTION-------------------------");
            e.printStackTrace();
            System.out.println("---------------------------END----------------------------");
        }
    }

    @After
    public void stopMongo() {
        try {
            System.out.println("DELETING OPLOG");
            FileUtils.deleteDirectory(new File(REPLICATION_PATH));
        } catch(Exception e) {}
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
        for(int i = 0; i < databases.size(); i++) {
            offsetMap.put(
                    Collections.singletonMap("mongodb", databases.get(i)),
                    Collections.singletonMap(databases.get(i), new Long(0))
            );
        }
        EasyMock.expect(context.offsetStorageReader()).andReturn(offsetStorageReader);
        EasyMock.expect(offsetStorageReader.offsets(EasyMock.anyObject(List.class))).andReturn(offsetMap);
    }
}
