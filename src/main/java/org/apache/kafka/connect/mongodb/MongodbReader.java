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
 * @author Andrea Patelli
 */
public class MongodbReader {
    private static final Logger log = LoggerFactory.getLogger(MongodbReader.class);

    protected ConcurrentLinkedQueue<Document> messages;

    private List<String> dbs;
    private String host;
    private Integer port;
    private Map<Map<String, String>, Map<String, Object>> start;

    public MongodbReader(String host, Integer port, List<String> dbs, Map<Map<String, String>, Map<String, Object>> start) {
        this.host = host;
        this.port = port;
        this.dbs = new ArrayList<>(0);
        this.dbs.addAll(dbs);
        this.start = start;
        this.messages = new ConcurrentLinkedQueue<>();
    }

    public void run() {
        for (String db : dbs) {
            String start;
            Map<String, Object> dbOffset = this.start.get(Collections.singletonMap("mongodb", db));
            if (dbOffset == null || dbOffset.isEmpty())
                start = "0";
            else
                start = (String) this.start.get(Collections.singletonMap("mongodb", db)).get(db);

            log.trace("Starting database reader with configuration: ");
            log.trace("host: {}", host);
            log.trace("port: {}", port);
            log.trace("db: {}", db);
            log.trace("start: {}", start);
            DatabaseReader reader = new DatabaseReader(host, port, db, start, messages);
            new Thread(reader).start();
        }
    }
}
