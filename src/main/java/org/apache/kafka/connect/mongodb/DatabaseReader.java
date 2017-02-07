package org.apache.kafka.connect.mongodb;

import com.mongodb.CursorType;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.connect.errors.ConnectException;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.BSONTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Reads mutation from a mongodb database
 *
 * @author Andrea Patelli
 */
public class DatabaseReader implements Runnable {
    private final Logger log = LoggerFactory.getLogger(DatabaseReader.class);
    private final String host;
    private final Integer port;
    private final String uri;
    private final String db;
    private MongoClient mongoClient;
    private String start;

    private ConcurrentLinkedQueue<Document> messages;

    private MongoCollection<Document> oplog;
    private Bson query;
    
    public DatabaseReader(String uri, String db, String start, ConcurrentLinkedQueue<Document> messages) {
        this.uri = uri;
        this.host = null;
        this.port = null;
        this.db = db;
        this.start = start;
        this.messages = messages;
        try {
            init();
        } catch (ConnectException e) {
            throw e;
        }
        log.trace("Starting from {}", start);
    }

    public DatabaseReader(String host, Integer port, String db, String start, ConcurrentLinkedQueue<Document> messages) {
        this.uri = null;
        this.host = host;
        this.port = port;
        this.db = db;
        this.start = start;
        this.messages = messages;
        try {
            init();
        } catch (ConnectException e) {
            throw e;
        }
        log.trace("Starting from {}", start);
    }

    @Override
    public void run() {
        Document fields = new Document();
        fields.put("ts", 1);
        fields.put("op", 1);
        fields.put("ns", 1);
        fields.put("o", 1);

        FindIterable<Document> documents = oplog
                .find(query)
                .sort(new Document("$natural", 1))
                .projection(Projections.include("ts", "op", "ns", "o"))
                .cursorType(CursorType.TailableAwait);

        try {
            for (Document document : documents) {
                log.trace(document.toString());
                messages.add(document);
            }
        } catch(Exception e) {
            log.error("Closed connection");
        }
    }

    private void init() {
        oplog = readCollection();
        query = createQuery();
    }
    
    @Override
    public void finalize(){
    	if(mongoClient != null){
    		mongoClient.close();
    	}
    }
    
    private MongoClient createMongoClient(){
    	MongoClient mongoClient;
    	if(uri != null){
    		final MongoClientURI mongoClientURI = new MongoClientURI(uri);
    		mongoClient = new MongoClient(mongoClientURI);
    	}
    	else{
    		mongoClient = new MongoClient(host, port);
    	}
		return mongoClient;
    }

    /**
     * Loads the oplog collection.
     *
     * @return the oplog collection
     */
    private MongoCollection<Document> readCollection() {
		mongoClient = createMongoClient();
		
        log.trace("Starting database reader with configuration: ");
		log.trace("addresses: {}", StringUtils.join(mongoClient.getAllAddress(), ","));
        log.trace("db: {}", db);
        log.trace("start: {}", start);
        
        final MongoDatabase db = mongoClient.getDatabase("local");
        return db.getCollection("oplog.rs");
    }

    /**
     * Creates the query to execute on the collection.
     *
     * @return the query
     */
    private Bson createQuery() {
        // timestamps are used as offsets, saved as a concatenation of seconds and order
        Long timestamp = Long.parseLong(start);
        Integer order = Long.valueOf(timestamp % 10).intValue();
        timestamp = timestamp / 10;

        Integer finalTimestamp = timestamp.intValue();
        Integer finalOrder = order;

        Bson query = Filters.and(
                Filters.exists("fromMigrate", false),
                Filters.gt("ts", new BSONTimestamp(finalTimestamp, finalOrder)),
                Filters.or(
                        Filters.eq("op", "i"),
                        Filters.eq("op", "u"),
                        Filters.eq("op", "d")
                ),
                Filters.eq("ns", db)
        );

        return query;
    }
}
