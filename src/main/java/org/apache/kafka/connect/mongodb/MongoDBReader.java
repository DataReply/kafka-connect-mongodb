package org.apache.kafka.connect.mongodb;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Class that creates a new DatabaseReader thread for every db
 *
 * @author Andrea Patelli
 */
public class MongoDBReader {
    private static final Logger log = LoggerFactory.getLogger(MongoDBReader.class);

    protected ConcurrentLinkedQueue<Document> messages;

    private List<String> dbs;
    private String hosts;
    private Map<Map<String, String>, Map<String, Object>> start;

    public MongoDBReader(String hosts, List<String> dbs, Map<Map<String, String>, Map<String, Object>> start) {
        this.hosts = hosts;
        this.dbs = new ArrayList<>(0);
        this.dbs.addAll(dbs);
        this.start = start;
        this.messages = new ConcurrentLinkedQueue<>();
    }

    public void run() {
        // for every database to watch
        dbs.stream().forEach(db -> {
            String start;
            // get the last message that was read
            Map<String, Object> dbOffset = this.start.get(Collections.singletonMap("mongodb", db));
            if (dbOffset == null || dbOffset.isEmpty())
                start = "0";
            else
                start = (String) this.start.get(Collections.singletonMap("mongodb", db)).get(db);

            log.trace("Starting database reader with configuration: ");
            log.trace("hosts: {}", hosts);
            log.trace("db: {}", db);
            log.trace("start: {}", start);
            // start a new thread for reading mutation of the specific database
            DatabaseReader reader = new DatabaseReader(hosts, db, start, messages);
            new Thread(reader).start();
        });
    }
}
