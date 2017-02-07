package org.apache.kafka.connect.mongodb;

import org.bson.Document;

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
public class MongodbReader {
    private final ConcurrentLinkedQueue<Document> messages;
    private final List<String> dbs;
    private final String uri;
    private final String host;
    private final Integer port;
    private final Integer batchSize;
    private final Map<Map<String, String>, Map<String, Object>> start;
    private final List<Thread> threads;

    public MongodbReader(String uri, List<String> dbs, Integer batchSize, Map<Map<String, String>, Map<String, Object>> start) {
        this.uri = uri;
        this.host = null;
        this.port = null;
        this.batchSize = batchSize;
        this.dbs = new ArrayList<>(0);
        this.dbs.addAll(dbs);
        this.threads = new ArrayList<>(0);
        this.start = start;
        this.messages = new ConcurrentLinkedQueue<>();
    }
    
    public MongodbReader(String host, Integer port, List<String> dbs, Integer batchSize, Map<Map<String, String>, Map<String, Object>> start) {
    	this.uri = null;
        this.host = host;
        this.port = port;
        this.batchSize = batchSize;
        this.dbs = new ArrayList<>(0);
        this.dbs.addAll(dbs);
        this.threads = new ArrayList<>(0);
        this.start = start;
        this.messages = new ConcurrentLinkedQueue<>();
    }

	public void run() {
        // for every database to watch
        for (String db : dbs) {            
            final String start = getStartOffset(db);
            
            DatabaseReader reader;
            if(uri != null){
            	reader = new DatabaseReader(uri, db, start, batchSize, messages);
            }
            else{
            	reader = new DatabaseReader(host, port, db, start, batchSize, messages);
            }
            final Thread thread = new Thread(reader);
			thread.start();
			threads.add(thread);
        }
    }
	

    private String getStartOffset(String db){
    	String start;
        final List<Map<String, String>> partitions = new ArrayList<>();
        final Map<String, String> partition = Collections.singletonMap("mongodb", db);
        partitions.add(partition);
     
        // get the last message that was read
        Map<String, Object> dbOffset = this.start.get(Collections.singletonMap("mongodb", db));
        if (dbOffset == null || dbOffset.isEmpty()){
            start = "0";
        }
        else{
            start = (String) this.start.get(Collections.singletonMap("mongodb", db)).get(db);
        }
        return start;
    }
    
	public void stop(){
    	for(Thread thread : threads){
    		thread.interrupt();
    	}
    }
    
	public boolean isEmpty(){
    	return messages.isEmpty();
    }

	public Document pool() {
		return messages.poll();
	}
}
